package com.unicom.edgenodech.bean;

/**
 * @author 作者 liushengjie
 * @version 创建时间：2017年5月19日 下午2:59:28 类说明
 */
public class Code {

	/**
	 * 成功
	 */
	public static final int SUCCESS = 0;

	/**
	 * 失败
	 */
	public static final int FAIL = 1;

	// motan框架异常
	public static final int MOTANEXCEPTION = 511;

	/**
	 * 参数错误: 参数不符合规范
	 */
	public static final int ARGUMENT_ERROR = 1000;

	public static final int UNAUTHORIZED = 401;

	/////////////// 鉴权服务//////////////////////////////////
	/**
	 * 用户名或密码错误
	 */
	public static final int ACCOUNT_PWD_ERROR = 2101;

	/**
	 * 账号已禁用
	 */
	public static final int ACCOUNT_FORBIDDEN = 2102;

	/**
	 * 设备已禁用
	 */
	public static final int DEVICE_FORBIDDEN = 2103;

	/**
	 * 连续密码错误15分钟后重试
	 */
	public static final int PWD_ERROR_CONTINUOUS = 2104;

	/////////////// 文件服务//////////////////////////////////

	/**
	 * 文件上传失败
	 */
	public static final int UPLOADFAIL = 7100;

	/**
	 * 文件上传失败
	 */
	public static final int FILENOTEXIST = 7101;

	///////////// 群组服务///////////////////////////////////

	/**
	 * 超过成员上限
	 */
	public static final int ABOVELIMIT = 3102;

	/**
	 * 成员数量不能为1
	 */
	public static final int NOTONE = 3104;

	/**
	 * 成员不能为空
	 */
	public static final int NOTEMPTY = 3103;

	/**
	 * 创建会话失败
	 */
	public static final int CREATEFAIL = 3101;

	/////////// im服务//////////////////////////////////////////

	/**
	 * 消息id非0
	 */
	public static final int MSGIDNOTZERO = 5101;

	/**
	 * 群组不存在
	 */
	public static final int GROUPNOTEXIST = 5102;

}
