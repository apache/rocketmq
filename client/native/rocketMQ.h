#ifndef ROCKETMQ_H_
#define ROCKETMQ_H_


typedef struct Message_Send{
  char* producer_name;
  char* topic;
  char* tags;
  char* keys;
  char* body;
}Message_Send_Struct;



#endif /* ROCKETMQ_H_ */