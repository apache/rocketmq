package com.alibaba.rocketmq.namesrv.common;

/**
 * @author lansheng.zj@taobao.com
 */
public abstract class MergeResult {
	/**
	 * 没有合并数据
	 */
	public static final int NOT_MERGE = 0; 
	/**
	 * 合并数据成功
	 */
	public static final int MERGE_SUCCESS = 1;
	/**
	 * 合并数据中出现无效数据
	 */
	public static final int MERGE_INVALID = 2;
	/**
	 * 系统错误
	 */
	public static final int SYS_ERROR = 3;
}
