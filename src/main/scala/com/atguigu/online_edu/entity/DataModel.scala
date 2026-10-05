package com.atguigu.online_edu.entity

case class RegisterLog (
                         userId:String,
                         ch:String,
                         createTime:String
                         )


case class QzLog (
                   userId:String,
                   courseId:String,
                   acknowledgeId:String,
                   questionId:String,
                   isRight:String,
                   createTime:String
                 )


//{"biz":"bdfb58e5-d14c-45d2-91bc-1d9409800ac3","chapterid":"1","cwareid":"3","edutypeid":"3","pe":"55","ps":"41","sourceType":"APP","speed":"2",
//	"subjectid":"2","te":"1563352166417","ts":"1563352159417","uid":"235","videoid":"2"}
case class VideoLog (
                      biz:String,
                      chapterid:String,
                      cwareid:String,
                      edutypeid:String,
                      pe:String,
                      ps:String,
                      sourceType:String,
                      speed:String,
                      subjectid:String,
                      te:String,
                      ts:String,
                      uid:String,
                      videoid:String,
                 )