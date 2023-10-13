package io.github.xiananliu.lock.consulLock;
import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.ecwid.consul.v1.session.model.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ConsulLock {
    protected Logger log = LoggerFactory.getLogger(getClass());

    private ConsulClient consulClient;
    private String sessionName;
    private String sessionId = null;
    private String lockKey;
    private Long lockTime;
    private CheckTtl checkTtl;
 
    /**
     * Consul 实现分布式锁
     * @param consulClient
     * @param lockKey       同步锁在consul的KV存储中的Key路径，会自动增加prefix前缀，方便归类查询
     */
    public ConsulLock(ConsulClient consulClient, String lockKey) {
        this.consulClient = consulClient;
        this.sessionName = "consul-lock-session-"+UUID.randomUUID();
        this.lockKey = lockKey;
    }
 
    /**
     * 获取同步锁
     *
     * @param block     是否阻塞，直到获取到锁为止
     * @return
     */
    public Boolean lock(boolean block) {

        if (sessionId != null) {
            throw new RuntimeException(sessionId + " - Already locked!");
        }
        sessionId = createSession(sessionName);
        while(true) {
            PutParams putParams = new PutParams();
            putParams.setAcquireSession(sessionId);

            if(consulClient.setKVValue(lockKey, "lock:" + LocalDateTime.now(), putParams).getValue()) {
                lockTime = System.currentTimeMillis();
                log.info("consul lock by:{}",sessionId);
                return true;
            } else {
//                log.info("current lock value:{}",consulClient.getKVValue(lockKey).getValue());
                if(block) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                       continue;
                    }
                    continue;
                }
                return false;
            }
        }
    }



 
    /**
     * 释放同步锁
     *
     * @return
     */
    public Boolean unlock() {
        if(checkTtl != null) {
            checkTtl.stop();
        }
        if (sessionId!=null) {
            PutParams putParams = new PutParams();
            putParams.setReleaseSession(sessionId);
            boolean result = consulClient.setKVValue(lockKey, "unlock:" + LocalDateTime.now(), putParams).getValue();
            consulClient.sessionDestroy(sessionId, null);
            log.info("consul unlock by:{}",sessionId);
            sessionId = null;
            return result;
        }
        return true;
    }


 
    /**
     * 创建session
     * @param sessionName
     * @return
     */
    private String createSession(String sessionName) {
        NewSession newSession = new NewSession();
        newSession.setName(sessionName);
        checkTtl = new CheckTtl("consul-lock-chek-"+UUID.randomUUID(),consulClient);
        checkTtl.start();
        // 如果有CheckTtl，就为该Session设置Check相关信息
        List<String> checks = new ArrayList<>();
        checks.add(checkTtl.getCheckId());
        newSession.setChecks(checks);
        newSession.setBehavior(Session.Behavior.DELETE);
        /** newSession.setTtl("60s");
         指定秒数（10s到86400s之间）。如果提供，在TTL到期之前没有更新，则会话无效。
         应使用最低的实际TTL来保持管理会话的数量。
         当锁被强制过期时，例如在领导选举期间，会话可能无法获得最多双倍TTL，
         因此应避免长TTL值（> 1小时）。**/
        return consulClient.sessionCreate(newSession, null).getValue();
    }

    public ConsulClient getConsulClient() {
        return consulClient;
    }

    public void setConsulClient(ConsulClient consulClient) {
        this.consulClient = consulClient;
    }

    public String getSessionName() {
        return sessionName;
    }

    public void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getLockKey() {
        return lockKey;
    }

    public void setLockKey(String lockKey) {
        this.lockKey = lockKey;
    }

    public Long getLockTime() {
        return lockTime;
    }

    public void setLockTime(Long lockTime) {
        this.lockTime = lockTime;
    }
}