import boto3


class EmailConnection:

    def __init__(self, aws_id, aws_secret):
        """
        Set parameters for the email to set default values
        """

        self.conn = boto3.client(
            'ses', region_name='us-west-2', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)

    def send_email(self, from_address=None, to_address_list=None, subject=None, body=None):
        """
        Send an email to approved emails
        :param from_address: Email address the email is sent from
        :param to_address_list: Email address list the email is sent to
        :param subject: str subject of email
        :param body: str body of email
        """
        try:
            self.conn.send_email(Source=from_address,
                                 Destination={'ToAddresses': to_address_list},
                                 Message={'Subject': {
                                     'Data': subject
                                 },
                                     'Body': {
                                         'Text': {
                                             'Data': body
                                         }
                                    }
                                 })
            print("Email sent to %s" % ', '.join(to_address_list))
        except Exception as e:
            print('error sending email, error: %s' % str(e))
            self.conn.send_email(Source='bi@hellofresh.com.au',
                                 Destination={'ToAddresses': [from_address] + to_address_list},
                                 Message={'Subject': {
                                     'Data': 'Error sending email'
                                 },
                                     'Body': {
                                         'Text': {
                                             'Data': 'Email sent from %s to %s with subject=%s and body=%s. '
                                                     'Check that user email address has been added to AWS SES. '
                                                     'Error message: %s'
                                                     % (from_address, ', '.join(to_address_list), subject, body,
                                                        str(e))
                                         }
                                     }
                                 })
