package io.github.xiananliu.lock.redisLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.TimeoutUtils;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class RedisLock implements Lock {

    private static final Logger logger = LoggerFactory.getLogger(RedisLock.class);

    /**
     * 重入计数器 大于0时代表持有锁
     */
    private AtomicInteger reentrantCount = new AtomicInteger(0);

    /**
     * 释放锁的lua脚本
     */
    private static String releaseScript = "local key= KEYS[1]\n"
            + "local lockId=ARGV[1]\n"
            + "\n"
            + "local result=0\n"
            + "local val= redis.call('get',key);\n"
            + "\n"
            + "if(not val) then\n"
            + "    result=0\n"
            + "elseif (val==lockId) then\n"
            + "    redis.call('del',key)\n"
            + "    result=1\n"
            + "else\n"
            + "    result=2\n"
            + "end\n"
            + "return result";

    /**
     * 刷新锁时间脚本
     */
    private static String refreshScript = "local key= KEYS[1]\n"
            + "local lockId=ARGV[1]\n"
            + "local expire = ARGV[2]\n"
            + "\n"
            + "local result=0\n"
            + "local val= redis.call('get',key);\n"
            + "\n"
            + "if(not val) then\n"
            + "    result=0\n"
            + "elseif (val==lockId) then\n"
            + "    redis.call('expire',key,expire)\n"
            + "    result=1\n"
            + "else\n"
            + "    result=2\n"
            + "end\n"
            + "return result";


    private StringRedisTemplate stringRedisTemplate;

    private String lockKey;

    private volatile String lockId;

    private RenewalTask renewalTask;

    /**
     * key超时时间
     */
    private int expireSeconds = 10;

    /**
     * 默认刷新周期，expireSeconds
     */
    private int refreshSeconds = 7;


    public RedisLock(StringRedisTemplate stringRedisTemplate, String lockKey) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.lockKey = lockKey;
    }


    @Override
    public void lock() {
        while (true) {
            if (tryLock()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
        //可重入
        synchronized (this) {
            if (reentrantCount.get() != 0) {
                reentrantCount.incrementAndGet();
                return true;
            }
        }
        lockId = UUID.randomUUID().toString();
        Boolean result = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, lockId);

        if (Boolean.TRUE.equals(result)) {
            stringRedisTemplate.expire(lockKey, expireSeconds, TimeUnit.SECONDS);
            logger.info("obtain lock success,lockKey:{},lockId:{},", lockKey, lockId);
            renewalTask = new RenewalTask();
            renewalTask.start();
            reentrantCount.set(1);
            return true;
        }
        return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        long start = System.currentTimeMillis();

        while (System.currentTimeMillis() - start < TimeoutUtils.toMillis(time, unit)) {
            if (tryLock()) {
                return true;
            }
            Thread.sleep(100);
        }
        return false;
    }

    @Override
    public void unlock() {
        //无锁
        synchronized (this) {
            if (reentrantCount.get() != 1) {
                reentrantCount.decrementAndGet();
                return;
            }
        }

        if (reentrantCount.get() == 0) {
            return;
        }

        renewalTask.stop();
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setResultType(Long.class);
        script.setScriptText(releaseScript);

        Long result = stringRedisTemplate
                .execute(script, Collections.singletonList(lockKey), lockId);
        reentrantCount.set(0);
        //成功
        if (result == 1) {
            logger.info("release lock success,lockKey:{},lockId:{},", lockKey, lockId);
            return;
        }
        //锁不存在
        if (result == 0) {
            logger.error("release lock error: lock not exist.lockKey:{},lockId:{},", lockKey, lockId);
        }

        if (result == 2) {
            logger.error("release lock error: lock by others,lockKey:{},lockId:{},", lockKey, lockId);
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }


    class RenewalTask {

        private Timer timer = new Timer();

        public void start() {
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    DefaultRedisScript<Long> script = new DefaultRedisScript<>();
                    script.setResultType(Long.class);
                    script.setScriptText(refreshScript);
                    Long result = stringRedisTemplate
                            .execute(script, Collections.singletonList(lockKey), lockId, String.valueOf(expireSeconds));
                    //成功
                    if (result == 1) {
                        logger.info("refresh lock success,lockKey:{},lockId:{},", lockKey, lockId);
                        return;
                    }

                    //锁不存在
                    if (result == 0) {
                        logger.error("refresh lock error: lock not exist,lockKey:{},lockId:{},", lockKey, lockId);
                    }

                    if (result == 2) {
                        logger.error("refresh lock error: lock by others,lockKey:{},lockId:{}", lockKey, lockId);
                    }

                    timer.cancel();

                }
            }, refreshSeconds * 1000L, refreshSeconds * 1000L);
        }

        public void stop() {
            //停止刷新
            timer.cancel();

        }
    }


}
