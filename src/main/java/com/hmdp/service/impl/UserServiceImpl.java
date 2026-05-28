package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j

public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {
        //1.校验手机号格式是否合法
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.格式不合法则返回错误信息
            return Result.fail("手机号格式出错，请重新输入");
        }
        //3.生成6位随机数字验证码
        String code = RandomUtil.randomNumbers(6);
        //4.保存验证码到Redis中，设置过期时间
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //5.模拟发送短信验证码
        log.info("短信验证码发送成功，验证码为{}", code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        String phone = loginForm.getPhone();
        //1.校验手机号格式是否合法
        if (RegexUtils.isPhoneInvalid(phone)) {
            //2.格式不合法则返回错误信息
            return Result.fail("手机号格式出错，请重新输入");
        }
        //3.从Redis获取验证码并进行校验
        String code = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        String code2 = loginForm.getCode();
        if (code == null || !code.equals(code2)) {
            return Result.fail("验证码错误");
        }
        //4.根据手机号查询用户信息
        User user = query().eq("phone", phone).one();

        //5.判断用户是否存在
        if (user == null) {
            //6.用户不存在，创建新用户并保存到数据库
            user = createWithPhone(phone);
        }
        //7.将用户信息保存到Redis中
        //7.1 生成随机token作为登录令牌
        String token = UUID.randomUUID().toString();
        //7.2 将User实体转换为UserDTO
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //7.3 将UserDTO转换为Map，并将所有字段值转为String类型以适配Redis存储
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        //7.4 将用户信息以Hash结构存储到Redis中
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        //7.5 设置token的过期时间
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);

        //8.返回token给前端
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String keySuffix=now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key =  USER_SIGN_KEY + userId + ":"+ keySuffix;
        //获取当天是本月的第几天
        int day = now.getDayOfMonth();
        //保存到数据库
        stringRedisTemplate.opsForValue().setBit(key, day - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //拼接key
        String keySuffix=now.format(DateTimeFormatter.ofPattern("yyyyMM"));
        String key =  USER_SIGN_KEY + userId + ":"+ keySuffix;
        //获取当天是本月的第几天
        int day = now.getDayOfMonth();
        //获取所有的签到天数
        List<Long> days = stringRedisTemplate.opsForValue()
                .bitField(key, BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(day)).valueAt(0));
        if (days == null || days.isEmpty() || days.get(0) == null) {
            //没有任何签到记录
            return Result.ok(0);
        }
        int count=0;
        //循环遍历
        Long signCount = days.get(0);
        while (true){
            //与1进行与运算
            if((signCount & 1)==0){
                break;
            }else {
                count++;
            }
            //将数字右移一位
            signCount = signCount >> 1;
        }
        return Result.ok(count);
    }


    /**
     * 根据手机号创建新用户
     * @param phone 手机号
     * @return 创建的用户对象
     */
    private User createWithPhone(String phone) {
        //1.创建用户对象
        User user = new User();
        //2.设置手机号
        user.setPhone(phone);
        //3.生成随机昵称（前缀+8位随机字符）
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(8));
        //4.保存到数据库
        save(user);
        return user;
    }
}
