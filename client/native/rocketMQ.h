#ifndef ROCKETMQ_H_
#define ROCKETMQ_H_


typedef struct Message_Send{
  char* producer_name;
  char* topic;
  char* tags;
  char* keys;
  char* body;
}Message_Send_Struct;

typedef struct Message_Listener{
	void (*f_callback_function)(void *thread, char* cstr);
}Message_Listener_Struct;
#endif /* ROCKETMQ_H_ */