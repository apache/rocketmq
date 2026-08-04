package apache.rocketmq.v2;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * Admin exposes control-plane operations for cluster administration and
 * diagnostics over gRPC, complementing the data-plane MessagingService.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.53.0)",
    comments = "Source: apache/rocketmq/v2/admin.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class AdminGrpc {

  private AdminGrpc() {}

  public static final String SERVICE_NAME = "apache.rocketmq.v2.Admin";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.ChangeLogLevelRequest,
      apache.rocketmq.v2.ChangeLogLevelResponse> getChangeLogLevelMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ChangeLogLevel",
      requestType = apache.rocketmq.v2.ChangeLogLevelRequest.class,
      responseType = apache.rocketmq.v2.ChangeLogLevelResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.ChangeLogLevelRequest,
      apache.rocketmq.v2.ChangeLogLevelResponse> getChangeLogLevelMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.ChangeLogLevelRequest, apache.rocketmq.v2.ChangeLogLevelResponse> getChangeLogLevelMethod;
    if ((getChangeLogLevelMethod = AdminGrpc.getChangeLogLevelMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getChangeLogLevelMethod = AdminGrpc.getChangeLogLevelMethod) == null) {
          AdminGrpc.getChangeLogLevelMethod = getChangeLogLevelMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.ChangeLogLevelRequest, apache.rocketmq.v2.ChangeLogLevelResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeLogLevel"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ChangeLogLevelRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ChangeLogLevelResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("ChangeLogLevel"))
              .build();
        }
      }
    }
    return getChangeLogLevelMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeTopicStatusRequest,
      apache.rocketmq.v2.DescribeTopicStatusResponse> getDescribeTopicStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribeTopicStatus",
      requestType = apache.rocketmq.v2.DescribeTopicStatusRequest.class,
      responseType = apache.rocketmq.v2.DescribeTopicStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeTopicStatusRequest,
      apache.rocketmq.v2.DescribeTopicStatusResponse> getDescribeTopicStatusMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeTopicStatusRequest, apache.rocketmq.v2.DescribeTopicStatusResponse> getDescribeTopicStatusMethod;
    if ((getDescribeTopicStatusMethod = AdminGrpc.getDescribeTopicStatusMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getDescribeTopicStatusMethod = AdminGrpc.getDescribeTopicStatusMethod) == null) {
          AdminGrpc.getDescribeTopicStatusMethod = getDescribeTopicStatusMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.DescribeTopicStatusRequest, apache.rocketmq.v2.DescribeTopicStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribeTopicStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DescribeTopicStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DescribeTopicStatusResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("DescribeTopicStatus"))
              .build();
        }
      }
    }
    return getDescribeTopicStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.ListSubscriptionRequest,
      apache.rocketmq.v2.ListSubscriptionResponse> getListSubscriptionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListSubscription",
      requestType = apache.rocketmq.v2.ListSubscriptionRequest.class,
      responseType = apache.rocketmq.v2.ListSubscriptionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.ListSubscriptionRequest,
      apache.rocketmq.v2.ListSubscriptionResponse> getListSubscriptionMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.ListSubscriptionRequest, apache.rocketmq.v2.ListSubscriptionResponse> getListSubscriptionMethod;
    if ((getListSubscriptionMethod = AdminGrpc.getListSubscriptionMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getListSubscriptionMethod = AdminGrpc.getListSubscriptionMethod) == null) {
          AdminGrpc.getListSubscriptionMethod = getListSubscriptionMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.ListSubscriptionRequest, apache.rocketmq.v2.ListSubscriptionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListSubscription"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ListSubscriptionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ListSubscriptionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("ListSubscription"))
              .build();
        }
      }
    }
    return getListSubscriptionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeSubscriptionRequest,
      apache.rocketmq.v2.DescribeSubscriptionResponse> getDescribeSubscriptionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribeSubscription",
      requestType = apache.rocketmq.v2.DescribeSubscriptionRequest.class,
      responseType = apache.rocketmq.v2.DescribeSubscriptionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeSubscriptionRequest,
      apache.rocketmq.v2.DescribeSubscriptionResponse> getDescribeSubscriptionMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeSubscriptionRequest, apache.rocketmq.v2.DescribeSubscriptionResponse> getDescribeSubscriptionMethod;
    if ((getDescribeSubscriptionMethod = AdminGrpc.getDescribeSubscriptionMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getDescribeSubscriptionMethod = AdminGrpc.getDescribeSubscriptionMethod) == null) {
          AdminGrpc.getDescribeSubscriptionMethod = getDescribeSubscriptionMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.DescribeSubscriptionRequest, apache.rocketmq.v2.DescribeSubscriptionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribeSubscription"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DescribeSubscriptionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DescribeSubscriptionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("DescribeSubscription"))
              .build();
        }
      }
    }
    return getDescribeSubscriptionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.DeleteSubscriptionRequest,
      apache.rocketmq.v2.DeleteSubscriptionResponse> getDeleteSubscriptionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DeleteSubscription",
      requestType = apache.rocketmq.v2.DeleteSubscriptionRequest.class,
      responseType = apache.rocketmq.v2.DeleteSubscriptionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.DeleteSubscriptionRequest,
      apache.rocketmq.v2.DeleteSubscriptionResponse> getDeleteSubscriptionMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.DeleteSubscriptionRequest, apache.rocketmq.v2.DeleteSubscriptionResponse> getDeleteSubscriptionMethod;
    if ((getDeleteSubscriptionMethod = AdminGrpc.getDeleteSubscriptionMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getDeleteSubscriptionMethod = AdminGrpc.getDeleteSubscriptionMethod) == null) {
          AdminGrpc.getDeleteSubscriptionMethod = getDeleteSubscriptionMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.DeleteSubscriptionRequest, apache.rocketmq.v2.DeleteSubscriptionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DeleteSubscription"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DeleteSubscriptionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DeleteSubscriptionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("DeleteSubscription"))
              .build();
        }
      }
    }
    return getDeleteSubscriptionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeGroupAccumulationRequest,
      apache.rocketmq.v2.DescribeGroupAccumulationResponse> getDescribeGroupAccumulationMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DescribeGroupAccumulation",
      requestType = apache.rocketmq.v2.DescribeGroupAccumulationRequest.class,
      responseType = apache.rocketmq.v2.DescribeGroupAccumulationResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeGroupAccumulationRequest,
      apache.rocketmq.v2.DescribeGroupAccumulationResponse> getDescribeGroupAccumulationMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.DescribeGroupAccumulationRequest, apache.rocketmq.v2.DescribeGroupAccumulationResponse> getDescribeGroupAccumulationMethod;
    if ((getDescribeGroupAccumulationMethod = AdminGrpc.getDescribeGroupAccumulationMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getDescribeGroupAccumulationMethod = AdminGrpc.getDescribeGroupAccumulationMethod) == null) {
          AdminGrpc.getDescribeGroupAccumulationMethod = getDescribeGroupAccumulationMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.DescribeGroupAccumulationRequest, apache.rocketmq.v2.DescribeGroupAccumulationResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DescribeGroupAccumulation"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DescribeGroupAccumulationRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.DescribeGroupAccumulationResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("DescribeGroupAccumulation"))
              .build();
        }
      }
    }
    return getDescribeGroupAccumulationMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.ListConsumerConnectionRequest,
      apache.rocketmq.v2.ListConsumerConnectionResponse> getListConsumerConnectionMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ListConsumerConnection",
      requestType = apache.rocketmq.v2.ListConsumerConnectionRequest.class,
      responseType = apache.rocketmq.v2.ListConsumerConnectionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.ListConsumerConnectionRequest,
      apache.rocketmq.v2.ListConsumerConnectionResponse> getListConsumerConnectionMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.ListConsumerConnectionRequest, apache.rocketmq.v2.ListConsumerConnectionResponse> getListConsumerConnectionMethod;
    if ((getListConsumerConnectionMethod = AdminGrpc.getListConsumerConnectionMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getListConsumerConnectionMethod = AdminGrpc.getListConsumerConnectionMethod) == null) {
          AdminGrpc.getListConsumerConnectionMethod = getListConsumerConnectionMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.ListConsumerConnectionRequest, apache.rocketmq.v2.ListConsumerConnectionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ListConsumerConnection"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ListConsumerConnectionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ListConsumerConnectionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("ListConsumerConnection"))
              .build();
        }
      }
    }
    return getListConsumerConnectionMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.ResetGroupOffsetRequest,
      apache.rocketmq.v2.ResetGroupOffsetResponse> getResetGroupOffsetMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResetGroupOffset",
      requestType = apache.rocketmq.v2.ResetGroupOffsetRequest.class,
      responseType = apache.rocketmq.v2.ResetGroupOffsetResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.ResetGroupOffsetRequest,
      apache.rocketmq.v2.ResetGroupOffsetResponse> getResetGroupOffsetMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.ResetGroupOffsetRequest, apache.rocketmq.v2.ResetGroupOffsetResponse> getResetGroupOffsetMethod;
    if ((getResetGroupOffsetMethod = AdminGrpc.getResetGroupOffsetMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getResetGroupOffsetMethod = AdminGrpc.getResetGroupOffsetMethod) == null) {
          AdminGrpc.getResetGroupOffsetMethod = getResetGroupOffsetMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.ResetGroupOffsetRequest, apache.rocketmq.v2.ResetGroupOffsetResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResetGroupOffset"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ResetGroupOffsetRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ResetGroupOffsetResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("ResetGroupOffset"))
              .build();
        }
      }
    }
    return getResetGroupOffsetMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.ListMessageRequest,
      apache.rocketmq.v2.ListMessageResponse> getQueryMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryMessage",
      requestType = apache.rocketmq.v2.ListMessageRequest.class,
      responseType = apache.rocketmq.v2.ListMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.ListMessageRequest,
      apache.rocketmq.v2.ListMessageResponse> getQueryMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.ListMessageRequest, apache.rocketmq.v2.ListMessageResponse> getQueryMessageMethod;
    if ((getQueryMessageMethod = AdminGrpc.getQueryMessageMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getQueryMessageMethod = AdminGrpc.getQueryMessageMethod) == null) {
          AdminGrpc.getQueryMessageMethod = getQueryMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.ListMessageRequest, apache.rocketmq.v2.ListMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ListMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.ListMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("QueryMessage"))
              .build();
        }
      }
    }
    return getQueryMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.PrintThreadStackTraceRequest,
      apache.rocketmq.v2.PrintThreadStackTraceResponse> getPrintThreadStackTraceMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PrintThreadStackTrace",
      requestType = apache.rocketmq.v2.PrintThreadStackTraceRequest.class,
      responseType = apache.rocketmq.v2.PrintThreadStackTraceResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.PrintThreadStackTraceRequest,
      apache.rocketmq.v2.PrintThreadStackTraceResponse> getPrintThreadStackTraceMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.PrintThreadStackTraceRequest, apache.rocketmq.v2.PrintThreadStackTraceResponse> getPrintThreadStackTraceMethod;
    if ((getPrintThreadStackTraceMethod = AdminGrpc.getPrintThreadStackTraceMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getPrintThreadStackTraceMethod = AdminGrpc.getPrintThreadStackTraceMethod) == null) {
          AdminGrpc.getPrintThreadStackTraceMethod = getPrintThreadStackTraceMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.PrintThreadStackTraceRequest, apache.rocketmq.v2.PrintThreadStackTraceResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PrintThreadStackTrace"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.PrintThreadStackTraceRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.PrintThreadStackTraceResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("PrintThreadStackTrace"))
              .build();
        }
      }
    }
    return getPrintThreadStackTraceMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.VerifyMessageRequest,
      apache.rocketmq.v2.VerifyMessageResponse> getVerifyMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "VerifyMessage",
      requestType = apache.rocketmq.v2.VerifyMessageRequest.class,
      responseType = apache.rocketmq.v2.VerifyMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.VerifyMessageRequest,
      apache.rocketmq.v2.VerifyMessageResponse> getVerifyMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.VerifyMessageRequest, apache.rocketmq.v2.VerifyMessageResponse> getVerifyMessageMethod;
    if ((getVerifyMessageMethod = AdminGrpc.getVerifyMessageMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getVerifyMessageMethod = AdminGrpc.getVerifyMessageMethod) == null) {
          AdminGrpc.getVerifyMessageMethod = getVerifyMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.VerifyMessageRequest, apache.rocketmq.v2.VerifyMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "VerifyMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.VerifyMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.VerifyMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("VerifyMessage"))
              .build();
        }
      }
    }
    return getVerifyMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.AdminSendMessageRequest,
      apache.rocketmq.v2.AdminSendMessageResponse> getAdminSendMessageMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AdminSendMessage",
      requestType = apache.rocketmq.v2.AdminSendMessageRequest.class,
      responseType = apache.rocketmq.v2.AdminSendMessageResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.AdminSendMessageRequest,
      apache.rocketmq.v2.AdminSendMessageResponse> getAdminSendMessageMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.AdminSendMessageRequest, apache.rocketmq.v2.AdminSendMessageResponse> getAdminSendMessageMethod;
    if ((getAdminSendMessageMethod = AdminGrpc.getAdminSendMessageMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getAdminSendMessageMethod = AdminGrpc.getAdminSendMessageMethod) == null) {
          AdminGrpc.getAdminSendMessageMethod = getAdminSendMessageMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.AdminSendMessageRequest, apache.rocketmq.v2.AdminSendMessageResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AdminSendMessage"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.AdminSendMessageRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.AdminSendMessageResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("AdminSendMessage"))
              .build();
        }
      }
    }
    return getAdminSendMessageMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.GetConsumerRunningInfoRequest,
      apache.rocketmq.v2.GetConsumerRunningInfoResponse> getGetConsumerRunningInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetConsumerRunningInfo",
      requestType = apache.rocketmq.v2.GetConsumerRunningInfoRequest.class,
      responseType = apache.rocketmq.v2.GetConsumerRunningInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.GetConsumerRunningInfoRequest,
      apache.rocketmq.v2.GetConsumerRunningInfoResponse> getGetConsumerRunningInfoMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.GetConsumerRunningInfoRequest, apache.rocketmq.v2.GetConsumerRunningInfoResponse> getGetConsumerRunningInfoMethod;
    if ((getGetConsumerRunningInfoMethod = AdminGrpc.getGetConsumerRunningInfoMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getGetConsumerRunningInfoMethod = AdminGrpc.getGetConsumerRunningInfoMethod) == null) {
          AdminGrpc.getGetConsumerRunningInfoMethod = getGetConsumerRunningInfoMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.GetConsumerRunningInfoRequest, apache.rocketmq.v2.GetConsumerRunningInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetConsumerRunningInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.GetConsumerRunningInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.GetConsumerRunningInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("GetConsumerRunningInfo"))
              .build();
        }
      }
    }
    return getGetConsumerRunningInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.GetTopicRouteRequest,
      apache.rocketmq.v2.GetTopicRouteResponse> getGetTopicRouteMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTopicRoute",
      requestType = apache.rocketmq.v2.GetTopicRouteRequest.class,
      responseType = apache.rocketmq.v2.GetTopicRouteResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.GetTopicRouteRequest,
      apache.rocketmq.v2.GetTopicRouteResponse> getGetTopicRouteMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.GetTopicRouteRequest, apache.rocketmq.v2.GetTopicRouteResponse> getGetTopicRouteMethod;
    if ((getGetTopicRouteMethod = AdminGrpc.getGetTopicRouteMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getGetTopicRouteMethod = AdminGrpc.getGetTopicRouteMethod) == null) {
          AdminGrpc.getGetTopicRouteMethod = getGetTopicRouteMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.GetTopicRouteRequest, apache.rocketmq.v2.GetTopicRouteResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTopicRoute"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.GetTopicRouteRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.GetTopicRouteResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("GetTopicRoute"))
              .build();
        }
      }
    }
    return getGetTopicRouteMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.QueryTimeSpanRequest,
      apache.rocketmq.v2.QueryTimeSpanResponse> getQueryTimeSpanMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "QueryTimeSpan",
      requestType = apache.rocketmq.v2.QueryTimeSpanRequest.class,
      responseType = apache.rocketmq.v2.QueryTimeSpanResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.QueryTimeSpanRequest,
      apache.rocketmq.v2.QueryTimeSpanResponse> getQueryTimeSpanMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.QueryTimeSpanRequest, apache.rocketmq.v2.QueryTimeSpanResponse> getQueryTimeSpanMethod;
    if ((getQueryTimeSpanMethod = AdminGrpc.getQueryTimeSpanMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getQueryTimeSpanMethod = AdminGrpc.getQueryTimeSpanMethod) == null) {
          AdminGrpc.getQueryTimeSpanMethod = getQueryTimeSpanMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.QueryTimeSpanRequest, apache.rocketmq.v2.QueryTimeSpanResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "QueryTimeSpan"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.QueryTimeSpanRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.QueryTimeSpanResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("QueryTimeSpan"))
              .build();
        }
      }
    }
    return getQueryTimeSpanMethod;
  }

  private static volatile io.grpc.MethodDescriptor<apache.rocketmq.v2.GetProxyRuntimeStatsRequest,
      apache.rocketmq.v2.GetProxyRuntimeStatsResponse> getGetProxyRuntimeStatsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetProxyRuntimeStats",
      requestType = apache.rocketmq.v2.GetProxyRuntimeStatsRequest.class,
      responseType = apache.rocketmq.v2.GetProxyRuntimeStatsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<apache.rocketmq.v2.GetProxyRuntimeStatsRequest,
      apache.rocketmq.v2.GetProxyRuntimeStatsResponse> getGetProxyRuntimeStatsMethod() {
    io.grpc.MethodDescriptor<apache.rocketmq.v2.GetProxyRuntimeStatsRequest, apache.rocketmq.v2.GetProxyRuntimeStatsResponse> getGetProxyRuntimeStatsMethod;
    if ((getGetProxyRuntimeStatsMethod = AdminGrpc.getGetProxyRuntimeStatsMethod) == null) {
      synchronized (AdminGrpc.class) {
        if ((getGetProxyRuntimeStatsMethod = AdminGrpc.getGetProxyRuntimeStatsMethod) == null) {
          AdminGrpc.getGetProxyRuntimeStatsMethod = getGetProxyRuntimeStatsMethod =
              io.grpc.MethodDescriptor.<apache.rocketmq.v2.GetProxyRuntimeStatsRequest, apache.rocketmq.v2.GetProxyRuntimeStatsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetProxyRuntimeStats"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.GetProxyRuntimeStatsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  apache.rocketmq.v2.GetProxyRuntimeStatsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AdminMethodDescriptorSupplier("GetProxyRuntimeStats"))
              .build();
        }
      }
    }
    return getGetProxyRuntimeStatsMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AdminStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AdminStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AdminStub>() {
        @java.lang.Override
        public AdminStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AdminStub(channel, callOptions);
        }
      };
    return AdminStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AdminBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AdminBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AdminBlockingStub>() {
        @java.lang.Override
        public AdminBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AdminBlockingStub(channel, callOptions);
        }
      };
    return AdminBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AdminFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AdminFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AdminFutureStub>() {
        @java.lang.Override
        public AdminFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AdminFutureStub(channel, callOptions);
        }
      };
    return AdminFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * Admin exposes control-plane operations for cluster administration and
   * diagnostics over gRPC, complementing the data-plane MessagingService.
   * </pre>
   */
  public static abstract class AdminImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Dynamically change the server log level.
     * </pre>
     */
    public void changeLogLevel(apache.rocketmq.v2.ChangeLogLevelRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ChangeLogLevelResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getChangeLogLevelMethod(), responseObserver);
    }

    /**
     * <pre>
     * Describe the status and metadata of a topic.
     * </pre>
     */
    public void describeTopicStatus(apache.rocketmq.v2.DescribeTopicStatusRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeTopicStatusResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDescribeTopicStatusMethod(), responseObserver);
    }

    /**
     * <pre>
     * List subscription relationships filtered by topic and/or group.
     * </pre>
     */
    public void listSubscription(apache.rocketmq.v2.ListSubscriptionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListSubscriptionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListSubscriptionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Describe subscriptions grouped per connected client.
     * </pre>
     */
    public void describeSubscription(apache.rocketmq.v2.DescribeSubscriptionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeSubscriptionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDescribeSubscriptionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Delete a subscription relationship.
     * </pre>
     */
    public void deleteSubscription(apache.rocketmq.v2.DeleteSubscriptionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DeleteSubscriptionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDeleteSubscriptionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Query the message accumulation (lag) of a consumer group.
     * </pre>
     */
    public void describeGroupAccumulation(apache.rocketmq.v2.DescribeGroupAccumulationRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeGroupAccumulationResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDescribeGroupAccumulationMethod(), responseObserver);
    }

    /**
     * <pre>
     * List online consumer connections of a group.
     * </pre>
     */
    public void listConsumerConnection(apache.rocketmq.v2.ListConsumerConnectionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListConsumerConnectionResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getListConsumerConnectionMethod(), responseObserver);
    }

    /**
     * <pre>
     * Reset the consume offset of a group to a timestamp.
     * </pre>
     */
    public void resetGroupOffset(apache.rocketmq.v2.ResetGroupOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ResetGroupOffsetResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getResetGroupOffsetMethod(), responseObserver);
    }

    /**
     * <pre>
     * Query messages by id, key or subscription within a time range.
     * </pre>
     */
    public void queryMessage(apache.rocketmq.v2.ListMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Print the thread stack trace of a client.
     * </pre>
     */
    public void printThreadStackTrace(apache.rocketmq.v2.PrintThreadStackTraceRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.PrintThreadStackTraceResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPrintThreadStackTraceMethod(), responseObserver);
    }

    /**
     * <pre>
     * Verify consumption of a message by a specific client.
     * </pre>
     */
    public void verifyMessage(apache.rocketmq.v2.VerifyMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.VerifyMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getVerifyMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Send a message from the admin side (e.g. a console test message).
     * </pre>
     */
    public void adminSendMessage(apache.rocketmq.v2.AdminSendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.AdminSendMessageResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAdminSendMessageMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch aggregated running information of a consumer client.
     * </pre>
     */
    public void getConsumerRunningInfo(apache.rocketmq.v2.GetConsumerRunningInfoRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetConsumerRunningInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetConsumerRunningInfoMethod(), responseObserver);
    }

    /**
     * <pre>
     * Query the route data of a topic.
     * </pre>
     */
    public void getTopicRoute(apache.rocketmq.v2.GetTopicRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetTopicRouteResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTopicRouteMethod(), responseObserver);
    }

    /**
     * <pre>
     * Query the time span of messages consumed by a group.
     * </pre>
     */
    public void queryTimeSpan(apache.rocketmq.v2.QueryTimeSpanRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.QueryTimeSpanResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getQueryTimeSpanMethod(), responseObserver);
    }

    /**
     * <pre>
     * Fetch runtime statistics of the serving process.
     * </pre>
     */
    public void getProxyRuntimeStats(apache.rocketmq.v2.GetProxyRuntimeStatsRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetProxyRuntimeStatsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetProxyRuntimeStatsMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getChangeLogLevelMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.ChangeLogLevelRequest,
                apache.rocketmq.v2.ChangeLogLevelResponse>(
                  this, METHODID_CHANGE_LOG_LEVEL)))
          .addMethod(
            getDescribeTopicStatusMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.DescribeTopicStatusRequest,
                apache.rocketmq.v2.DescribeTopicStatusResponse>(
                  this, METHODID_DESCRIBE_TOPIC_STATUS)))
          .addMethod(
            getListSubscriptionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.ListSubscriptionRequest,
                apache.rocketmq.v2.ListSubscriptionResponse>(
                  this, METHODID_LIST_SUBSCRIPTION)))
          .addMethod(
            getDescribeSubscriptionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.DescribeSubscriptionRequest,
                apache.rocketmq.v2.DescribeSubscriptionResponse>(
                  this, METHODID_DESCRIBE_SUBSCRIPTION)))
          .addMethod(
            getDeleteSubscriptionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.DeleteSubscriptionRequest,
                apache.rocketmq.v2.DeleteSubscriptionResponse>(
                  this, METHODID_DELETE_SUBSCRIPTION)))
          .addMethod(
            getDescribeGroupAccumulationMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.DescribeGroupAccumulationRequest,
                apache.rocketmq.v2.DescribeGroupAccumulationResponse>(
                  this, METHODID_DESCRIBE_GROUP_ACCUMULATION)))
          .addMethod(
            getListConsumerConnectionMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.ListConsumerConnectionRequest,
                apache.rocketmq.v2.ListConsumerConnectionResponse>(
                  this, METHODID_LIST_CONSUMER_CONNECTION)))
          .addMethod(
            getResetGroupOffsetMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.ResetGroupOffsetRequest,
                apache.rocketmq.v2.ResetGroupOffsetResponse>(
                  this, METHODID_RESET_GROUP_OFFSET)))
          .addMethod(
            getQueryMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.ListMessageRequest,
                apache.rocketmq.v2.ListMessageResponse>(
                  this, METHODID_QUERY_MESSAGE)))
          .addMethod(
            getPrintThreadStackTraceMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.PrintThreadStackTraceRequest,
                apache.rocketmq.v2.PrintThreadStackTraceResponse>(
                  this, METHODID_PRINT_THREAD_STACK_TRACE)))
          .addMethod(
            getVerifyMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.VerifyMessageRequest,
                apache.rocketmq.v2.VerifyMessageResponse>(
                  this, METHODID_VERIFY_MESSAGE)))
          .addMethod(
            getAdminSendMessageMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.AdminSendMessageRequest,
                apache.rocketmq.v2.AdminSendMessageResponse>(
                  this, METHODID_ADMIN_SEND_MESSAGE)))
          .addMethod(
            getGetConsumerRunningInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.GetConsumerRunningInfoRequest,
                apache.rocketmq.v2.GetConsumerRunningInfoResponse>(
                  this, METHODID_GET_CONSUMER_RUNNING_INFO)))
          .addMethod(
            getGetTopicRouteMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.GetTopicRouteRequest,
                apache.rocketmq.v2.GetTopicRouteResponse>(
                  this, METHODID_GET_TOPIC_ROUTE)))
          .addMethod(
            getQueryTimeSpanMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.QueryTimeSpanRequest,
                apache.rocketmq.v2.QueryTimeSpanResponse>(
                  this, METHODID_QUERY_TIME_SPAN)))
          .addMethod(
            getGetProxyRuntimeStatsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                apache.rocketmq.v2.GetProxyRuntimeStatsRequest,
                apache.rocketmq.v2.GetProxyRuntimeStatsResponse>(
                  this, METHODID_GET_PROXY_RUNTIME_STATS)))
          .build();
    }
  }

  /**
   * <pre>
   * Admin exposes control-plane operations for cluster administration and
   * diagnostics over gRPC, complementing the data-plane MessagingService.
   * </pre>
   */
  public static final class AdminStub extends io.grpc.stub.AbstractAsyncStub<AdminStub> {
    private AdminStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AdminStub(channel, callOptions);
    }

    /**
     * <pre>
     * Dynamically change the server log level.
     * </pre>
     */
    public void changeLogLevel(apache.rocketmq.v2.ChangeLogLevelRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ChangeLogLevelResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getChangeLogLevelMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Describe the status and metadata of a topic.
     * </pre>
     */
    public void describeTopicStatus(apache.rocketmq.v2.DescribeTopicStatusRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeTopicStatusResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDescribeTopicStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List subscription relationships filtered by topic and/or group.
     * </pre>
     */
    public void listSubscription(apache.rocketmq.v2.ListSubscriptionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListSubscriptionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListSubscriptionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Describe subscriptions grouped per connected client.
     * </pre>
     */
    public void describeSubscription(apache.rocketmq.v2.DescribeSubscriptionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeSubscriptionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDescribeSubscriptionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Delete a subscription relationship.
     * </pre>
     */
    public void deleteSubscription(apache.rocketmq.v2.DeleteSubscriptionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DeleteSubscriptionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDeleteSubscriptionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Query the message accumulation (lag) of a consumer group.
     * </pre>
     */
    public void describeGroupAccumulation(apache.rocketmq.v2.DescribeGroupAccumulationRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeGroupAccumulationResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDescribeGroupAccumulationMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * List online consumer connections of a group.
     * </pre>
     */
    public void listConsumerConnection(apache.rocketmq.v2.ListConsumerConnectionRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListConsumerConnectionResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getListConsumerConnectionMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Reset the consume offset of a group to a timestamp.
     * </pre>
     */
    public void resetGroupOffset(apache.rocketmq.v2.ResetGroupOffsetRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ResetGroupOffsetResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getResetGroupOffsetMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Query messages by id, key or subscription within a time range.
     * </pre>
     */
    public void queryMessage(apache.rocketmq.v2.ListMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Print the thread stack trace of a client.
     * </pre>
     */
    public void printThreadStackTrace(apache.rocketmq.v2.PrintThreadStackTraceRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.PrintThreadStackTraceResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPrintThreadStackTraceMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Verify consumption of a message by a specific client.
     * </pre>
     */
    public void verifyMessage(apache.rocketmq.v2.VerifyMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.VerifyMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getVerifyMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Send a message from the admin side (e.g. a console test message).
     * </pre>
     */
    public void adminSendMessage(apache.rocketmq.v2.AdminSendMessageRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.AdminSendMessageResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAdminSendMessageMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch aggregated running information of a consumer client.
     * </pre>
     */
    public void getConsumerRunningInfo(apache.rocketmq.v2.GetConsumerRunningInfoRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetConsumerRunningInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetConsumerRunningInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Query the route data of a topic.
     * </pre>
     */
    public void getTopicRoute(apache.rocketmq.v2.GetTopicRouteRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetTopicRouteResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTopicRouteMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Query the time span of messages consumed by a group.
     * </pre>
     */
    public void queryTimeSpan(apache.rocketmq.v2.QueryTimeSpanRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.QueryTimeSpanResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getQueryTimeSpanMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     * Fetch runtime statistics of the serving process.
     * </pre>
     */
    public void getProxyRuntimeStats(apache.rocketmq.v2.GetProxyRuntimeStatsRequest request,
        io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetProxyRuntimeStatsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetProxyRuntimeStatsMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * Admin exposes control-plane operations for cluster administration and
   * diagnostics over gRPC, complementing the data-plane MessagingService.
   * </pre>
   */
  public static final class AdminBlockingStub extends io.grpc.stub.AbstractBlockingStub<AdminBlockingStub> {
    private AdminBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AdminBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Dynamically change the server log level.
     * </pre>
     */
    public apache.rocketmq.v2.ChangeLogLevelResponse changeLogLevel(apache.rocketmq.v2.ChangeLogLevelRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getChangeLogLevelMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Describe the status and metadata of a topic.
     * </pre>
     */
    public apache.rocketmq.v2.DescribeTopicStatusResponse describeTopicStatus(apache.rocketmq.v2.DescribeTopicStatusRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDescribeTopicStatusMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List subscription relationships filtered by topic and/or group.
     * </pre>
     */
    public apache.rocketmq.v2.ListSubscriptionResponse listSubscription(apache.rocketmq.v2.ListSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListSubscriptionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Describe subscriptions grouped per connected client.
     * </pre>
     */
    public apache.rocketmq.v2.DescribeSubscriptionResponse describeSubscription(apache.rocketmq.v2.DescribeSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDescribeSubscriptionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Delete a subscription relationship.
     * </pre>
     */
    public apache.rocketmq.v2.DeleteSubscriptionResponse deleteSubscription(apache.rocketmq.v2.DeleteSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDeleteSubscriptionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query the message accumulation (lag) of a consumer group.
     * </pre>
     */
    public apache.rocketmq.v2.DescribeGroupAccumulationResponse describeGroupAccumulation(apache.rocketmq.v2.DescribeGroupAccumulationRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDescribeGroupAccumulationMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * List online consumer connections of a group.
     * </pre>
     */
    public apache.rocketmq.v2.ListConsumerConnectionResponse listConsumerConnection(apache.rocketmq.v2.ListConsumerConnectionRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getListConsumerConnectionMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Reset the consume offset of a group to a timestamp.
     * </pre>
     */
    public apache.rocketmq.v2.ResetGroupOffsetResponse resetGroupOffset(apache.rocketmq.v2.ResetGroupOffsetRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getResetGroupOffsetMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query messages by id, key or subscription within a time range.
     * </pre>
     */
    public apache.rocketmq.v2.ListMessageResponse queryMessage(apache.rocketmq.v2.ListMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Print the thread stack trace of a client.
     * </pre>
     */
    public apache.rocketmq.v2.PrintThreadStackTraceResponse printThreadStackTrace(apache.rocketmq.v2.PrintThreadStackTraceRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPrintThreadStackTraceMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Verify consumption of a message by a specific client.
     * </pre>
     */
    public apache.rocketmq.v2.VerifyMessageResponse verifyMessage(apache.rocketmq.v2.VerifyMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getVerifyMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Send a message from the admin side (e.g. a console test message).
     * </pre>
     */
    public apache.rocketmq.v2.AdminSendMessageResponse adminSendMessage(apache.rocketmq.v2.AdminSendMessageRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAdminSendMessageMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch aggregated running information of a consumer client.
     * </pre>
     */
    public apache.rocketmq.v2.GetConsumerRunningInfoResponse getConsumerRunningInfo(apache.rocketmq.v2.GetConsumerRunningInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetConsumerRunningInfoMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query the route data of a topic.
     * </pre>
     */
    public apache.rocketmq.v2.GetTopicRouteResponse getTopicRoute(apache.rocketmq.v2.GetTopicRouteRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTopicRouteMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Query the time span of messages consumed by a group.
     * </pre>
     */
    public apache.rocketmq.v2.QueryTimeSpanResponse queryTimeSpan(apache.rocketmq.v2.QueryTimeSpanRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getQueryTimeSpanMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     * Fetch runtime statistics of the serving process.
     * </pre>
     */
    public apache.rocketmq.v2.GetProxyRuntimeStatsResponse getProxyRuntimeStats(apache.rocketmq.v2.GetProxyRuntimeStatsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetProxyRuntimeStatsMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * Admin exposes control-plane operations for cluster administration and
   * diagnostics over gRPC, complementing the data-plane MessagingService.
   * </pre>
   */
  public static final class AdminFutureStub extends io.grpc.stub.AbstractFutureStub<AdminFutureStub> {
    private AdminFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AdminFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AdminFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Dynamically change the server log level.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.ChangeLogLevelResponse> changeLogLevel(
        apache.rocketmq.v2.ChangeLogLevelRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getChangeLogLevelMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Describe the status and metadata of a topic.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.DescribeTopicStatusResponse> describeTopicStatus(
        apache.rocketmq.v2.DescribeTopicStatusRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDescribeTopicStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List subscription relationships filtered by topic and/or group.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.ListSubscriptionResponse> listSubscription(
        apache.rocketmq.v2.ListSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListSubscriptionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Describe subscriptions grouped per connected client.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.DescribeSubscriptionResponse> describeSubscription(
        apache.rocketmq.v2.DescribeSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDescribeSubscriptionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Delete a subscription relationship.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.DeleteSubscriptionResponse> deleteSubscription(
        apache.rocketmq.v2.DeleteSubscriptionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDeleteSubscriptionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Query the message accumulation (lag) of a consumer group.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.DescribeGroupAccumulationResponse> describeGroupAccumulation(
        apache.rocketmq.v2.DescribeGroupAccumulationRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDescribeGroupAccumulationMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * List online consumer connections of a group.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.ListConsumerConnectionResponse> listConsumerConnection(
        apache.rocketmq.v2.ListConsumerConnectionRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getListConsumerConnectionMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Reset the consume offset of a group to a timestamp.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.ResetGroupOffsetResponse> resetGroupOffset(
        apache.rocketmq.v2.ResetGroupOffsetRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getResetGroupOffsetMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Query messages by id, key or subscription within a time range.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.ListMessageResponse> queryMessage(
        apache.rocketmq.v2.ListMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Print the thread stack trace of a client.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.PrintThreadStackTraceResponse> printThreadStackTrace(
        apache.rocketmq.v2.PrintThreadStackTraceRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPrintThreadStackTraceMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Verify consumption of a message by a specific client.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.VerifyMessageResponse> verifyMessage(
        apache.rocketmq.v2.VerifyMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getVerifyMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Send a message from the admin side (e.g. a console test message).
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.AdminSendMessageResponse> adminSendMessage(
        apache.rocketmq.v2.AdminSendMessageRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAdminSendMessageMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch aggregated running information of a consumer client.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.GetConsumerRunningInfoResponse> getConsumerRunningInfo(
        apache.rocketmq.v2.GetConsumerRunningInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetConsumerRunningInfoMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Query the route data of a topic.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.GetTopicRouteResponse> getTopicRoute(
        apache.rocketmq.v2.GetTopicRouteRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTopicRouteMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Query the time span of messages consumed by a group.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.QueryTimeSpanResponse> queryTimeSpan(
        apache.rocketmq.v2.QueryTimeSpanRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getQueryTimeSpanMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     * Fetch runtime statistics of the serving process.
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<apache.rocketmq.v2.GetProxyRuntimeStatsResponse> getProxyRuntimeStats(
        apache.rocketmq.v2.GetProxyRuntimeStatsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetProxyRuntimeStatsMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CHANGE_LOG_LEVEL = 0;
  private static final int METHODID_DESCRIBE_TOPIC_STATUS = 1;
  private static final int METHODID_LIST_SUBSCRIPTION = 2;
  private static final int METHODID_DESCRIBE_SUBSCRIPTION = 3;
  private static final int METHODID_DELETE_SUBSCRIPTION = 4;
  private static final int METHODID_DESCRIBE_GROUP_ACCUMULATION = 5;
  private static final int METHODID_LIST_CONSUMER_CONNECTION = 6;
  private static final int METHODID_RESET_GROUP_OFFSET = 7;
  private static final int METHODID_QUERY_MESSAGE = 8;
  private static final int METHODID_PRINT_THREAD_STACK_TRACE = 9;
  private static final int METHODID_VERIFY_MESSAGE = 10;
  private static final int METHODID_ADMIN_SEND_MESSAGE = 11;
  private static final int METHODID_GET_CONSUMER_RUNNING_INFO = 12;
  private static final int METHODID_GET_TOPIC_ROUTE = 13;
  private static final int METHODID_QUERY_TIME_SPAN = 14;
  private static final int METHODID_GET_PROXY_RUNTIME_STATS = 15;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AdminImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(AdminImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CHANGE_LOG_LEVEL:
          serviceImpl.changeLogLevel((apache.rocketmq.v2.ChangeLogLevelRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.ChangeLogLevelResponse>) responseObserver);
          break;
        case METHODID_DESCRIBE_TOPIC_STATUS:
          serviceImpl.describeTopicStatus((apache.rocketmq.v2.DescribeTopicStatusRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeTopicStatusResponse>) responseObserver);
          break;
        case METHODID_LIST_SUBSCRIPTION:
          serviceImpl.listSubscription((apache.rocketmq.v2.ListSubscriptionRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListSubscriptionResponse>) responseObserver);
          break;
        case METHODID_DESCRIBE_SUBSCRIPTION:
          serviceImpl.describeSubscription((apache.rocketmq.v2.DescribeSubscriptionRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeSubscriptionResponse>) responseObserver);
          break;
        case METHODID_DELETE_SUBSCRIPTION:
          serviceImpl.deleteSubscription((apache.rocketmq.v2.DeleteSubscriptionRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.DeleteSubscriptionResponse>) responseObserver);
          break;
        case METHODID_DESCRIBE_GROUP_ACCUMULATION:
          serviceImpl.describeGroupAccumulation((apache.rocketmq.v2.DescribeGroupAccumulationRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.DescribeGroupAccumulationResponse>) responseObserver);
          break;
        case METHODID_LIST_CONSUMER_CONNECTION:
          serviceImpl.listConsumerConnection((apache.rocketmq.v2.ListConsumerConnectionRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListConsumerConnectionResponse>) responseObserver);
          break;
        case METHODID_RESET_GROUP_OFFSET:
          serviceImpl.resetGroupOffset((apache.rocketmq.v2.ResetGroupOffsetRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.ResetGroupOffsetResponse>) responseObserver);
          break;
        case METHODID_QUERY_MESSAGE:
          serviceImpl.queryMessage((apache.rocketmq.v2.ListMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.ListMessageResponse>) responseObserver);
          break;
        case METHODID_PRINT_THREAD_STACK_TRACE:
          serviceImpl.printThreadStackTrace((apache.rocketmq.v2.PrintThreadStackTraceRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.PrintThreadStackTraceResponse>) responseObserver);
          break;
        case METHODID_VERIFY_MESSAGE:
          serviceImpl.verifyMessage((apache.rocketmq.v2.VerifyMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.VerifyMessageResponse>) responseObserver);
          break;
        case METHODID_ADMIN_SEND_MESSAGE:
          serviceImpl.adminSendMessage((apache.rocketmq.v2.AdminSendMessageRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.AdminSendMessageResponse>) responseObserver);
          break;
        case METHODID_GET_CONSUMER_RUNNING_INFO:
          serviceImpl.getConsumerRunningInfo((apache.rocketmq.v2.GetConsumerRunningInfoRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetConsumerRunningInfoResponse>) responseObserver);
          break;
        case METHODID_GET_TOPIC_ROUTE:
          serviceImpl.getTopicRoute((apache.rocketmq.v2.GetTopicRouteRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetTopicRouteResponse>) responseObserver);
          break;
        case METHODID_QUERY_TIME_SPAN:
          serviceImpl.queryTimeSpan((apache.rocketmq.v2.QueryTimeSpanRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.QueryTimeSpanResponse>) responseObserver);
          break;
        case METHODID_GET_PROXY_RUNTIME_STATS:
          serviceImpl.getProxyRuntimeStats((apache.rocketmq.v2.GetProxyRuntimeStatsRequest) request,
              (io.grpc.stub.StreamObserver<apache.rocketmq.v2.GetProxyRuntimeStatsResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class AdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AdminBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return apache.rocketmq.v2.MQAdmin.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Admin");
    }
  }

  private static final class AdminFileDescriptorSupplier
      extends AdminBaseDescriptorSupplier {
    AdminFileDescriptorSupplier() {}
  }

  private static final class AdminMethodDescriptorSupplier
      extends AdminBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    AdminMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (AdminGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AdminFileDescriptorSupplier())
              .addMethod(getChangeLogLevelMethod())
              .addMethod(getDescribeTopicStatusMethod())
              .addMethod(getListSubscriptionMethod())
              .addMethod(getDescribeSubscriptionMethod())
              .addMethod(getDeleteSubscriptionMethod())
              .addMethod(getDescribeGroupAccumulationMethod())
              .addMethod(getListConsumerConnectionMethod())
              .addMethod(getResetGroupOffsetMethod())
              .addMethod(getQueryMessageMethod())
              .addMethod(getPrintThreadStackTraceMethod())
              .addMethod(getVerifyMessageMethod())
              .addMethod(getAdminSendMessageMethod())
              .addMethod(getGetConsumerRunningInfoMethod())
              .addMethod(getGetTopicRouteMethod())
              .addMethod(getQueryTimeSpanMethod())
              .addMethod(getGetProxyRuntimeStatsMethod())
              .build();
        }
      }
    }
    return result;
  }
}
