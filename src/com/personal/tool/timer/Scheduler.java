package com.personal.tool.timer;

import com.personal.tool.timer.util.StrUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Scheduler {

    /**
     * 定时任务表
     */
    protected final TaskTable taskTable = new TaskTable();
    private final Lock lock = new ReentrantLock();
    /**
     * 是否为守护线程
     */
    protected boolean daemon;
    /**
     * 执行器管理器
     */
    protected TaskExecutorManager taskExecutorManager;
    /**
     * 监听管理器列表
     */
    protected TaskListenerManager listenerManager = new TaskListenerManager();
    /**
     * 线程池，用于执行 {@link TaskExecutor}
     */
    protected ExecutorService threadExecutor;
    /**
     * 是否已经启动
     */
    private boolean started = false;

    /**
     * 增加监听器
     *
     * @param listener {@link TaskListener}
     * @return this
     */
    public Scheduler addListener(TaskListener listener) {
        this.listenerManager.addListener(listener);
        return this;
    }

    /**
     * 移除监听器
     *
     * @param listener {@link TaskListener}
     * @return this
     */
    public Scheduler removeListener(TaskListener listener) {
        this.listenerManager.removeListener(listener);
        return this;
    }

    public interface TaskListener {
        /**
         * 定时任务启动时触发
         *
         * @param executor {@link TaskExecutor}
         */
        void onStart(TaskExecutor executor);

        /**
         * 任务成功结束时触发
         *
         * @param executor {@link TaskExecutor}
         */
        void onSucceeded(TaskExecutor executor);

        /**
         * 任务启动失败时触发
         *
         * @param executor  {@link TaskExecutor}
         * @param exception 异常
         */
        void onFailed(TaskExecutor executor, Throwable exception);
    }

    @FunctionalInterface
    public interface Task {

        /**
         * 执行作业
         * <p>
         * 作业的具体实现需考虑异常情况，默认情况下任务异常在监听中统一监听处理，如果不加入监听，异常会被忽略<br>
         * 因此最好自行捕获异常后处理
         */
        void execute();
    }

    /**
     * 定时作业，此类除了定义了作业，也定义了作业的执行周期以及ID。
     *
     * @author looly
     * @since 5.4.7
     */
    public static class CronTask implements Task {

        private final String id;
        private final Task task;
        private CronPattern pattern;

        /**
         * 构造
         *
         * @param id      ID
         * @param pattern 表达式
         * @param task    作业
         */
        public CronTask(String id, CronPattern pattern, Task task) {
            this.id = id;
            this.pattern = pattern;
            this.task = task;
        }

        @Override
        public void execute() {
            task.execute();
        }

        /**
         * 获取作业ID
         *
         * @return 作业ID
         */
        public String getId() {
            return id;
        }

        /**
         * 获取表达式
         *
         * @return 表达式
         */
        public CronPattern getPattern() {
            return pattern;
        }

        /**
         * 设置新的定时表达式
         *
         * @param pattern 表达式
         * @return this
         */
        public CronTask setPattern(CronPattern pattern) {
            this.pattern = pattern;
            return this;
        }

        /**
         * 获取原始作业
         *
         * @return 作业
         */
        public Task getRaw() {
            return this.task;
        }
    }

    private static class TaskListenerManager {
        private final List<TaskListener> listeners = new ArrayList<>();

        /**
         * add a default listener
         */
        public TaskListenerManager() {
            listeners.add(new TaskListener() {
                @Override
                public void onStart(TaskExecutor executor) {
                    System.out.println("on start " + executor);
                }

                @Override
                public void onSucceeded(TaskExecutor executor) {
                    System.out.println("on succeeded " + executor);
                }

                @Override
                public void onFailed(TaskExecutor executor, Throwable exception) {
                    System.err.println(exception.getClass().getName() + ":" + exception.getMessage());
                }
            });
        }

        /**
         * 增加监听器,ignore nulls.
         *
         * @param listener {@link TaskListener}
         * @return this
         */
        public TaskListenerManager addListener(TaskListener listener) {
            if (listener == null) return this;
            synchronized (listeners) {
                listeners.add(listener);
            }
            return this;
        }

        /**
         * 移除监听器
         *
         * @param listener {@link TaskListener}
         * @return this
         */
        public TaskListenerManager removeListener(TaskListener listener) {
            synchronized (listeners) {
                listeners.remove(listener);
            }
            return this;
        }

        /**
         * 通知所有监听任务启动器启动
         *
         * @param executor {@link TaskExecutor}
         */
        public void notifyTaskStart(TaskExecutor executor) {
            for (TaskListener listener : listeners) listener.onStart(executor);
        }

        /**
         * 通知所有监听任务启动器成功结束
         *
         * @param executor {@link TaskExecutor}
         */
        public void notifyTaskSucceeded(TaskExecutor executor) {
            for (TaskListener listener : listeners) listener.onSucceeded(executor);
        }

        /**
         * 通知所有监听任务启动器结束并失败
         *
         * @param executor  {@link TaskExecutor}
         * @param exception 失败原因
         */
        public void notifyTaskFailed(TaskExecutor executor, Throwable exception) {
            for (TaskListener listener : listeners) listener.onFailed(executor, exception);
        }
    }

    public static class TaskTable {
        public static final int DEFAULT_SIZE = 10;

        private final ReadWriteLock lock;

        private final List<String> ids;
        private final List<CronPattern> patterns;
        private final List<Task> tasks;
        private int size;

        public TaskTable() {
            this(DEFAULT_SIZE);
        }

        public TaskTable(int initialCapacity) {
            lock = new ReentrantReadWriteLock();

            ids = new ArrayList<>(initialCapacity);
            patterns = new ArrayList<>(initialCapacity);
            tasks = new ArrayList<>(initialCapacity);
        }

        /**
         * 新增Task
         *
         * @param id      ID
         * @param pattern {@link CronPattern}
         * @param task    {@link Task}
         * @return this
         */
        public TaskTable add(String id, CronPattern pattern, Task task) {
            final Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                if (ids.contains(id)) {
                    throw new CronException("Id [{}] has been existed!", id);
                }
                ids.add(id);
                patterns.add(pattern);
                tasks.add(task);
                size++;
            } finally {
                writeLock.unlock();
            }
            return this;
        }

        /**
         * 获取所有ID，返回不可变列表，即列表不可修改
         *
         * @return ID列表
         * @since 4.6.7
         */
        public List<String> getIds() {
            final Lock readLock = lock.readLock();
            readLock.lock();
            try {
                return Collections.unmodifiableList(this.ids);
            } finally {
                readLock.unlock();
            }
        }

        /**
         * 获取所有定时任务表达式，返回不可变列表，即列表不可修改
         *
         * @return 定时任务表达式列表
         * @since 4.6.7
         */
        public List<CronPattern> getPatterns() {
            final Lock readLock = lock.readLock();
            readLock.lock();
            try {
                return Collections.unmodifiableList(this.patterns);
            } finally {
                readLock.unlock();
            }
        }

        /**
         * 获取所有定时任务，返回不可变列表，即列表不可修改
         *
         * @return 定时任务列表
         * @since 4.6.7
         */
        public List<Task> getTasks() {
            final Lock readLock = lock.readLock();
            readLock.lock();
            try {
                return Collections.unmodifiableList(this.tasks);
            } finally {
                readLock.unlock();
            }
        }

        /**
         * 移除Task
         *
         * @param id Task的ID
         */
        public void remove(String id) {
            final Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                final int index = ids.indexOf(id);
                if (index > -1) {
                    tasks.remove(index);
                    patterns.remove(index);
                    ids.remove(index);
                    size--;
                }
            } finally {
                writeLock.unlock();
            }
        }

        /**
         * 更新某个Task的定时规则
         *
         * @param id      Task的ID
         * @param pattern 新的表达式
         * @return 是否更新成功，如果id对应的规则不存在则不更新
         * @since 4.0.10
         */
        public boolean updatePattern(String id, CronPattern pattern) {
            final Lock writeLock = lock.writeLock();
            writeLock.lock();
            try {
                final int index = ids.indexOf(id);
                if (index > -1) {
                    patterns.set(index, pattern);
                    return true;
                }
            } finally {
                writeLock.unlock();
            }
            return false;
        }

        /**
         * 获得指定位置的{@link Task}
         *
         * @param index 位置
         * @return {@link Task}
         * @since 3.1.1
         */
        public Task getTask(int index) {
            final Lock readLock = lock.readLock();
            readLock.lock();
            try {
                return tasks.get(index);
            } finally {
                readLock.unlock();
            }
        }

        /**
         * 获得指定id的{@link Task}
         *
         * @param id ID
         * @return {@link Task}
         * @since 3.1.1
         */
        public Task getTask(String id) {
            final int index = ids.indexOf(id);
            if (index > -1) {
                return getTask(index);
            }
            return null;
        }

        /**
         * 获得指定位置的{@link CronPattern}
         *
         * @param index 位置
         * @return {@link CronPattern}
         * @since 3.1.1
         */
        public CronPattern getPattern(int index) {
            final Lock readLock = lock.readLock();
            readLock.lock();
            try {
                return patterns.get(index);
            } finally {
                readLock.unlock();
            }
        }

        /**
         * 任务表大小，加入的任务数
         *
         * @return 任务表大小，加入的任务数
         * @since 4.0.2
         */
        public int size() {
            return this.size;
        }

        /**
         * 任务表是否为空
         *
         * @return true为空
         * @since 4.0.2
         */
        public boolean isEmpty() {
            return this.size < 1;
        }

        /**
         * 获得指定id的{@link CronPattern}
         *
         * @param id ID
         * @return {@link CronPattern}
         * @since 3.1.1
         */
        public CronPattern getPattern(String id) {
            final int index = ids.indexOf(id);
            if (index > -1) {
                return getPattern(index);
            }
            return null;
        }

        /**
         * 如果时间匹配则执行相应的Task，带读锁
         *
         * @param scheduler {@link Scheduler}
         * @param millis    时间毫秒
         */
        public void executeTaskIfMatch(Scheduler scheduler, long millis) {
            final Lock readLock = lock.readLock();
            readLock.lock();
            try {
                executeTaskIfMatchInternal(scheduler, millis);
            } finally {
                readLock.unlock();
            }
        }

        /**
         * 如果时间匹配则执行相应的Task，无锁
         *
         * @param scheduler {@link Scheduler}
         * @param millis    时间毫秒
         * @since 3.1.1
         */
        protected void executeTaskIfMatchInternal(Scheduler scheduler, long millis) {
            for (int i = 0; i < size; i++) {
                if (patterns.get(i).match(scheduler.config.timezone, millis, scheduler.config.matchSecond)) {
                    scheduler.taskExecutorManager.spawnExecutor(new CronTask(ids.get(i), patterns.get(i), tasks.get(i)));
                }
            }
        }
    }

    /**
     * 作业执行管理器<br>
     * 负责管理作业的启动、停止等
     * <p>
     * 此类用于管理正在运行的作业情况，作业启动后加入任务列表，任务结束移除
     * </p>
     */
    private static class TaskExecutorManager {
        /**
         * 执行器列表
         */
        private final List<TaskExecutor> executors = new ArrayList<>();

        protected Scheduler scheduler;

        public TaskExecutorManager(Scheduler scheduler) {
            this.scheduler = scheduler;
        }

        /**
         * 获取所有正在执行的任务调度执行器
         *
         * @return 任务执行器列表
         * @since 4.6.7
         */
        public List<TaskExecutor> getExecutors() {
            return Collections.unmodifiableList(this.executors);
        }

        /**
         * 启动 执行器TaskExecutor，即启动作业
         *
         * @param task {@link Task}
         * @return {@link TaskExecutor}
         */
        public TaskExecutor spawnExecutor(CronTask task) {
            final TaskExecutor executor = new TaskExecutor(this.scheduler, task);
            synchronized (this.executors) {
                this.executors.add(executor);
            }
            // 子线程是否为deamon线程取决于父线程，因此此处无需显示调用
            // executor.setDaemon(this.scheduler.daemon);
//		executor.start();
            this.scheduler.threadExecutor.execute(executor);
            return executor;
        }

        /**
         * 执行器执行完毕调用此方法，将执行器从执行器列表移除<br/>
         * <p>
         * 此方法由{@link TaskExecutor}对象调用，用于通知管理器自身已完成执行
         *
         * @param executor 执行器 {@link TaskExecutor}
         * @return this
         */
        public TaskExecutorManager notifyExecutorCompleted(TaskExecutor executor) {
            synchronized (executors) {
                executors.remove(executor);
            }
            return this;
        }

        /**
         * 停止所有TaskExecutor
         *
         * @return this
         * @deprecated 作业执行器只是执行给定的定时任务线程，无法强制关闭，可通过deamon线程方式关闭之
         */
        @Deprecated
        public TaskExecutorManager destroy() {
            // synchronized (this.executors) {
            // for (TaskExecutor taskExecutor : executors) {
            // ThreadUtil.interupt(taskExecutor, false);
            // }
            // }
            this.executors.clear();
            return this;
        }
    }

    public static class TaskExecutor implements Runnable {

        private final Scheduler scheduler;
        private final CronTask task;

        /**
         * 构造
         *
         * @param scheduler 调度器
         * @param task      被执行的任务
         */
        public TaskExecutor(Scheduler scheduler, CronTask task) {
            this.scheduler = scheduler;
            this.task = task;
        }

        /**
         * 获得原始任务对象
         *
         * @return 任务对象
         */
        public Task getTask() {
            return this.task.getRaw();
        }

        /**
         * 获得原始任务对象
         *
         * @return 任务对象
         * @since 5.4.7
         */
        public CronTask getCronTask() {
            return this.task;
        }

        @Override
        public void run() {
            try {
                scheduler.listenerManager.notifyTaskStart(this);
                task.execute();
                scheduler.listenerManager.notifyTaskSucceeded(this);
            } catch (Exception e) {
                scheduler.listenerManager.notifyTaskFailed(this, e);
            } finally {
                scheduler.taskExecutorManager.notifyExecutorCompleted(this);
            }
        }
    }

    private static class CronPattern {
    }

    public static class CronException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public CronException(Throwable e) { super(e); }

        public CronException(String message) { super(message); }

        public CronException(String messageTemplate, Object... params) {
            super(StrUtil.format(messageTemplate, params));
        }

        public CronException(Throwable throwable, String messageTemplate, Object... params) {
            super(StrUtil.format(messageTemplate, params), throwable);
        }
    }
}
