package com.alibaba.rocketmq.example.simple;

import com.alibaba.rocketmq.client.producer.LocalTransactionExecuter;

public class TranExecuterImpl implements LocalTransactionExecuter{
	private int transactionStats=-1;//0 send ,1 commit,2 rollback
	@Override
	public boolean executeLocalTransactionBranch() {
		if (transactionStats ==1){
		// TODO Auto-generated method stub
			return true;
		}else if(transactionStats ==2){
			return false;
		}
		return false;
	}
	public int getTransactionStats() {
		return transactionStats;
	}
	public void setTransactionStats(int transactionStats) {
		this.transactionStats = transactionStats;
	}

}
