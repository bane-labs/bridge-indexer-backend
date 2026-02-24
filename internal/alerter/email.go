package alerter

import (
	"context"
	"fmt"
	"net/smtp"
	"strings"
)

type EmailAlerter struct {
	host       string
	port       int
	user       string
	pass       string
	from       string
	recipients []string
}

func NewEmailAlerter(host string, port int, user, pass, from string, recipients []string) *EmailAlerter {
	return &EmailAlerter{
		host:       host,
		port:       port,
		user:       user,
		pass:       pass,
		from:       from,
		recipients: recipients,
	}
}

func (e *EmailAlerter) Send(ctx context.Context, alert Alert) error {
	subject := fmt.Sprintf("[%s] %s — %s", strings.ToUpper(alert.Severity), alert.Title, alert.BridgeType)
	body := fmt.Sprintf("Subject: %s\r\nFrom: %s\r\nTo: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\n\nBridge: %s\nSeverity: %s\nTime: %s",
		subject,
		e.from,
		strings.Join(e.recipients, ", "),
		alert.Message,
		alert.BridgeType,
		alert.Severity,
		alert.Timestamp.String(),
	)

	if alert.Operation != nil {
		body += fmt.Sprintf("\nNonce: %d\nDirection: %s\nSource TX: %s",
			alert.Operation.Nonce,
			alert.Operation.Direction,
			alert.Operation.SourceTxHash,
		)
	}

	addr := fmt.Sprintf("%s:%d", e.host, e.port)
	var auth smtp.Auth
	if e.user != "" {
		auth = smtp.PlainAuth("", e.user, e.pass, e.host)
	}

	return smtp.SendMail(addr, auth, e.from, e.recipients, []byte(body))
}

func (e *EmailAlerter) BuildMessage(alert Alert) string {
	subject := fmt.Sprintf("[%s] %s — %s", strings.ToUpper(alert.Severity), alert.Title, alert.BridgeType)
	return fmt.Sprintf("Subject: %s\r\nFrom: %s\r\nTo: %s\r\n\r\n%s",
		subject, e.from, strings.Join(e.recipients, ", "), alert.Message)
}
