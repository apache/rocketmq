#include "org_apache_rocketmq_common_UtilAll.h"
#include <unistd.h>
JNIEXPORT jint JNICALL Java_org_apache_rocketmq_common_UtilAll_get_1pid(JNIEnv *env, jclass klass) {
    return getpid();
}