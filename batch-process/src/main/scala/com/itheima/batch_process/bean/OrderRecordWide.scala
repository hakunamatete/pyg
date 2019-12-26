package com.itheima.batch_process.bean

case class OrderRecordWide(benefitAmount:Double,        // 红包金额
                           orderAmount:Double,          // 订单金额
                           payAmount:Double,            // 支付金额
                           activityNum:String,          // 获取ID
                           createTime:String,           // 订单创建时间
                           merchantId:String,           // 商家ID
                           orderId:String,              // 订单ID
                           payTime:String,              // 支付时间
                           payMethod:String,            // 支付方式
                           voucherAmount:String,        // 优惠券金额
                           commodityId:String,          // 产品ID
                           userId:String,               // 用户ID
                           yearMonthDay:String,         // 年月日
                           yearMonth:String,            // 年月
                           year:String                  // 年
                          )
