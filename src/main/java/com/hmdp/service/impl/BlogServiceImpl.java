package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import io.netty.util.internal.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Resource
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IFollowService followService;
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog ->{
           this.queryBlogUserId(blog);
           this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result likeBlog(Long id) {
        //判断用户是否点赞过了
        String key=BLOG_LIKED_KEY+id;
        Long userId = UserHolder.getUser().getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if(score==null){
            //没有点赞过，可以点赞
            boolean isSuccess = update().setSql("liked=liked+1").eq("id", id).update();
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key, userId.toString(),System.currentTimeMillis());
            }
        }else {
            //若已经点赞过，再次点击取消点赞
            boolean isSuccess = update().setSql("liked=liked-1").eq("id", id).update();
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        String key = BLOG_LIKED_KEY + id;
        //查询redis数据库
        Set<String> set = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        //获取用户id
        //添加非空判断
        if (set==null||set.isEmpty())   {
            return Result.ok();
        }
        List<Long> list = set.stream().map(Long::valueOf).collect(Collectors.toList());
        //根据用户id查询用户信息
      //将list集合拼接为字符串，中间使用逗号连接
        String idStr = StrUtil.join(",", list);
        List<UserDTO> ids = userService.query()
                .in("id",list)
                .last("ORDER BY FIELD(id,"+ idStr +")")
                .list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        //返回
        return  Result.ok(ids);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess) {
            return Result.fail("发布失败");
        }
        //获取登录用户的全部粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //向全部粉丝发送推送
        follows.forEach(follow -> {
            //获取粉丝id
            Long userId = follow.getUserId();
            String key=FEED_KEY+userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(),System.currentTimeMillis());
        });
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long maxtime, Integer offset) {
        // 1. 获取当前登录用户ID，构造个人收件箱Redis Key
        Long userId = UserHolder.getUser().getId();
        String key = FEED_KEY + userId;
            
        // 2. 从Redis收件箱（ZSet）中查询关注用户的博客ID，按时间戳倒序分页获取
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .rangeByScoreWithScores(key, 0, maxtime, offset, 3);
    
        // 3. 判空处理：若收件箱为空，直接返回
        if (typedTuples == null || typedTuples.isEmpty()) {
            return Result.ok();
        }
            
        // 4. 解析ZSet数据，提取博客ID列表、最小时间戳（用于下次查询）、偏移量（用于去重）
        List<String> idStr = new ArrayList<>();
        Long minTime = 0L;
        int count = 1;
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            // 获取博客ID
            String value = tuple.getValue();
            idStr.add(value);
                
            // 获取时间戳（作为ZSet的score）
            long time = tuple.getScore().longValue();
            if (minTime == time) {
                // 时间戳相同，累加偏移量（避免滚动加载时重复数据）
                count++;
            } else {
                // 更新最小时间戳，重置偏移量
                minTime = time;
                count = 1;
            }
        }
            
        // 5. 根据博客ID列表批量查询博客详情，并按原始顺序排序
        List<Blog> blogs = query()
                .in("id", idStr)
                .last("ORDER BY FIELD(id," + StrUtil.join(",", idStr) + ")")
                .list();
            
        // 6. 填充博客的附加信息：用户信息、点赞状态
        for (Blog blog : blogs) {
            queryBlogUserId(blog);  // 查询并填充博主信息
            isBlogLiked(blog);      // 查询当前用户是否已点赞
        }
            
        // 7. 封装滚动分页结果（博客列表、最小时间戳、偏移量）
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(count);
        scrollResult.setMinTime(minTime);
            
        // 8. 返回结果
        return Result.ok(scrollResult);
    }

    @Override
    public Result queryById(Long id) {
        //查询博客信息
        Blog blog = getById(id);
        if (blog==null){
            return Result.fail("笔记不存在");
        }
        //查询用户信息
        queryBlogUserId(blog);
        //查询用户是否点过赞
        isBlogLiked(blog);

        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        String key=BLOG_LIKED_KEY+blog.getId();
        Long userId = UserHolder.getUser().getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score!=null){
            blog.setIsLike(true);
        }
    }

    private void queryBlogUserId(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

}
