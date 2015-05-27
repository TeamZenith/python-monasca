import smtplib
import sys
import email.mime.text

def send_emails(to_addrs, subject, content):
    """Used for development and testing."""
    mail_username='monasca.notification@gmail.com'
    mail_password='notification'
    from_addr = mail_username

    # HOST & PORT
    HOST = 'smtp.gmail.com'
    PORT = 25

    # Create SMTP Object
    smtp = smtplib.SMTP()
    print 'connecting ...'

    # show the debug log
    smtp.set_debuglevel(1)

    # connect

    try:
        print smtp.connect(HOST,PORT)
    except:
        print 'CONNECT ERROR ****'
    # gmail uses ssl
    smtp.starttls()
    # login with username & password
    try:
        print 'loginning ...'
        smtp.login(mail_username,mail_password)
    except:
        print 'LOGIN ERROR ****'
    # fill content with MIMEText's object
    msg = email.mime.text.MIMEText(content)
    msg['From'] = from_addr
    msg['To'] = ';'.join(to_addrs)
    msg['Subject'] = subject
    print msg.as_string()
    smtp.sendmail(from_addr,to_addrs,msg.as_string())
    smtp.quit()