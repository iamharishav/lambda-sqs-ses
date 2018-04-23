var AWS = require('aws-sdk');
var SQS = new AWS.SQS({region: "us-east-1"});
var S3 = new AWS.S3();
var async = require('async');
var mark = require('markup-js');

var SES = new AWS.SES({
  accessKeyId: '...................',
  secretAccesskey: '------------------',
  region: '.......' 
});

var queueURL = ".....................................";

function receiveMessages(callback) {
  var params = {
    QueueUrl: queueURL,
    MaxNumberOfMessages: 10
  };
  SQS.receiveMessage(params, function(err, data) {
    if (err) {
      console.error(err, err.stack);
      callback(err);
    } else {
      callback(null, data.Messages);
    }
  });
}

function invokeWorkerLambda(message, callback) {
  var messageBodyAttributes = JSON.parse(message.Body);
  messageBodyAttributes.rootPath = process.env.rootPath;
  S3.getObject({
    Bucket: ".............", 
    Key: messageBodyAttributes.templateId
  }, function (err, data) {
    if (err) {
      console.log(err, err.stack);
      context.fail('Internal Error: Failed to load template from s3.');
    } else {
      var templateBody = data.Body.toString();
      var messageBody = mark.up(templateBody, messageBodyAttributes); 
      var emailParams = {
        Destination: {
            ToAddresses: [
              messageBodyAttributes.email_address
            ]
        },
        Message: {
            
            Subject: {
                Data: messageBodyAttributes.email_subject,
                Charset: 'UTF-8'
            }
        },
        Source: "no-reply@domain.com",
        ReplyToAddresses: [
            "......" + '<' + 'info@domain.com' + '>'
        ]
      };
            
      emailParams.Message.Body = {
          Html: {
              Data: messageBody,
              Charset: 'UTF-8'
          }
      };
      var email = SES.sendEmail(emailParams, function(err, data){
        if(err) {
          callback(err);
        }else {
          var deleteParams = {
            QueueUrl: queueURL,
            ReceiptHandle: message.ReceiptHandle
          };
          SQS.deleteMessage(deleteParams, function(err, data) {
            if (err) {
              //callback(err);
            } else {
              //callback(null, 'DONE');
            }
          });
        }
      });
    }
  });
};

function handleSQSMessages(context, callback) {
  receiveMessages(function(err, messages) {
    if (messages && messages.length > 0) {
      var invocations = [];
      messages.forEach(function(message) {
        invocations.push(function(callback) {
          invokeWorkerLambda(message, callback);
        });
      });
       async.parallel(invocations, function(err) {
         if (err) {
           callback(err);
         } else {
           if (context.getRemainingTimeInMillis() > 20000) {
            handleSQSMessages(context, callback); 
           } else {
             callback(null, 'PAUSE');
           }         
         }
       });
    } else {
      callback(null, 'DONE');
    }
  });
}
exports.handler = (event, context, callback) => {
  handleSQSMessages(context, callback);
};