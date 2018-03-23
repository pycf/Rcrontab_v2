import smtplib
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr


class SendMail:
    def __init__(self, to_addr, msg):
        self.to_addr = to_addr
        self.msg = msg

    @staticmethod
    def _format_addr(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))

    def send_mail(self) :
        msg = MIMEText(self.msg, 'plain', 'utf-8')
        from_addr = 'huruizhi@pystandard.com'
        password = '510543763@hrz'
        smtp_server = 'smtp.mxhichina.com'
        to_addr = self.to_addr
        msg['From'] = self._format_addr('Crontab_Robot <%s>' % from_addr)
        msg['To'] = self._format_addr('管理员 <%s>' % to_addr)
        msg['Subject'] = Header('来自普益crontab的邮件……', 'utf-8').encode()
        server = smtplib.SMTP(smtp_server, 25)
        # SMTP协议默认端口是25
        server.set_debuglevel(1)
        server.login(from_addr, password)
        server.sendmail(from_addr, [to_addr], msg.as_string())
        server.quit()


if __name__ == '__main__':
    msg = 'Hello , send by Python...'
    to_addr = '510543763@qq.com,denglei@pystandard.com'
    send_mail = SendMail(to_addr, msg)
    send_mail.send_mail()
