package com.unicom.edgenodech.bean;

import java.io.Serializable;

/**
 * @author 作者 liushengjie
 * @version 创建时间：2017年5月19日 下午2:59:22 类说明
 */
public class DataResponse extends BaseResponse implements Serializable {

	private static final long serialVersionUID = 1L;

	private Object data;

	public DataResponse() {
		super();
	}

	public DataResponse(Object data) {
		this.data = data;
	}

	public DataResponse(Integer errno, String error) {
		super(errno, error);
	}

	public DataResponse(Boolean success, Integer errno, String error) {
		super(success, errno, error);
	}

	/**
	 * @return data
	 */
	public Object getData() {
		return data;
	}

	/**
	 * @param data
	 *            要设置的 data
	 */
	public void setData(Object data) {
		this.data = data;
	}

}
