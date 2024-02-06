using Fase2_Global.Importadores;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Mail;
using System.Net.Mime;
using System.Text;

namespace Fase2_Global.Utilidades
{
    public class Email
    {
        private IConfiguration Configuration;
        private ILogger<Importador> Logger;

        public Email(ILogger<Importador> logger, IConfiguration configuration)
        {
            this.Logger = logger;
            this.Configuration = configuration;
        }

        public void Execute(string sMessage, string tipoCarga)
        {
            // string path = @"proceso_carga_finalizado.html";
            string subject = $"{sMessage}";
            //string body = $"Cuerpo del mensaje del correo: {sMessage}";
            string body = "La carga de la Fase 1 ha sido procesada y finalizada";
            SmtpClient client = new SmtpClient(Configuration.GetSection("SMTP").GetValue<string>("host"));
            MailMessage message;
            client.Credentials = new NetworkCredential(Configuration.GetSection("SMTP").GetValue<string>("username"), Configuration.GetSection("SMTP").GetValue<string>("password"));
            message = new MailMessage();
            message.From = new MailAddress(Configuration.GetSection("SMTP").GetValue<string>("from"));
            foreach (var to in Configuration.GetSection("SMTP").GetValue<string>("to").Split(';').ToList())
                message.Bcc.Add(to);
            message.Body = body;
            //      message.BodyEncoding = Encoding.GetEncoding(1252);
            message.IsBodyHtml = true;
            //       var htmlView = AlternateView.CreateAlternateViewFromString(body, new ContentType("text/html"));
            //      htmlView.ContentType.CharSet = Encoding.UTF8.WebName;
            //      message.AlternateViews.Add(htmlView);
            message.Subject = subject;
            client.Send(message);
        }
        public void Send(Exception e)
        {
            try
            {
                SmtpClient client = new SmtpClient(Configuration.GetSection("SMTP").GetValue<string>("host"));
                MailMessage message;

                client.Credentials = new NetworkCredential(Configuration.GetSection("SMTP").GetValue<string>("username"), Configuration.GetSection("SMTP").GetValue<string>("password"));
                message = new MailMessage();

                message.From = new MailAddress(Configuration.GetSection("SMTP").GetValue<string>("from"));
                foreach (var to in Configuration.GetSection("SMTP").GetValue<string>("to").Split(';').ToList())
                    message.Bcc.Add(to);

                var body = e.ToString();
                var subject = "** ERROR NO ENERGETICOS **";

                message.Body = body;
                message.IsBodyHtml = true;
                message.Subject = subject;

                client.Send(message);
            }
            catch (Exception)
            {
                Logger.LogError($"Error al enviar correo: {e.Message}");
            }
        }
    }
}
