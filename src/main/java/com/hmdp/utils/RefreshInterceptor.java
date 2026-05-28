package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

/**
 * 登录拦截器：基于Token验证用户身份
 */
public class RefreshInterceptor implements HandlerInterceptor {

    private StringRedisTemplate stringRedisTemplate;

    /**
     * 构造器注入StringRedisTemplate
     * @param stringRedisTemplate Redis操作模板
     */
    public RefreshInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.从请求头中获取token
        String token = request.getHeader("authorization");
        if (token == null){
            return true;
        }

        //2.基于token从Redis中查询用户信息
        Map<Object, Object> entries = stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY + token);

        //3.判断用户是否存在（Redis中是否有对应的用户数据）
        if (entries.isEmpty()) {
            //4.用户不存在，返回401未授权状态并拦截请求
            return true;
        }

        //5.将Redis中的HashMap数据转换为UserDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(entries, new UserDTO(), false);
        //6.将用户信息保存到ThreadLocal中，供后续业务使用
        UserHolder.saveUser(userDTO);

        //7.刷新token的过期时间（实现自动续期功能）
        stringRedisTemplate.expire(LOGIN_USER_KEY + token, LOGIN_USER_TTL, TimeUnit.MINUTES);

        //8.放行请求
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}
