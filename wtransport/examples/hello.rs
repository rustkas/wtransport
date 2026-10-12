//! WebTransport echo-сервер на 127.0.0.1:4433/echo.
//!
//! Отвечает эхом на датаграммы и bidirectional-стримы.
//! Запуск из корня репозитория:
//!     cargo run --example hello

use anyhow::Result;
use wtransport::endpoint::IncomingSession;
use wtransport::{Endpoint, Identity, ServerConfig};

const PORT: u16 = 4433;
const PATH: &str = "/echo";

#[tokio::main]
async fn main() -> Result<()> {
    let identity = Identity::load_pemfiles("cert.pem", "key.pem").await?;
    let config = ServerConfig::builder()
        .with_bind_default(PORT)
        .with_identity(identity)
        .build();
    let endpoint = Endpoint::server(config)?;

    println!("=== WebTransport echo server ===");
    println!("URL:  https://127.0.0.1:{PORT}{PATH}");
    println!("Порт: {PORT}");
    println!();
    println!("ВАЖНО:");
    println!("  • Используйте Chrome или Edge. Firefox не подойдёт.");
    println!("  • Включите chrome://flags → WebTransport Developer Mode.");
    println!("  • Откройте https://127.0.0.1:{PORT}{PATH} в Chrome");
    println!("    и нажмите Advanced → Proceed, чтобы принять сертификат.");
    println!();
    println!("  Ошибка 'This site can't be reached' при открытии URL");
    println!("  в браузере — ЭТО НОРМАЛЬНО. Сервер отвечает только");
    println!("  по QUIC/UDP, HTTP-запросы не обрабатывает.");
    println!();

    loop {
        let incoming = endpoint.accept().await;
        tokio::spawn(handle_session(incoming));
    }
}

async fn handle_session(incoming: IncomingSession) {
    let request = match incoming.await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("[{}] accept incoming: {e}", now_hms());
            return;
        }
    };

        // Проверяем path: /echo.
    // Превращаем в owned String до accept() — иначе borrow
    // переживёт перемещение request и компилятор не пропустит.
    let path: String = request.path().to_string();
    if path != PATH {
        eprintln!("[{}] отказ: path = {path}", now_hms());
        return;
    }

    let connection = match request.accept().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[{}] accept session: {e}", now_hms());
            return;
        }
    };

    println!("[{}] клиент подключён (path={path})", now_hms());

    let conn_dg = connection.clone();
    let conn_bi = connection.clone();

    // Датаграммы: эхо.
    let dg_task = tokio::spawn(async move {
        loop {
            match conn_dg.receive_datagram().await {
                Ok(dg) => {
                    let payload = dg.payload();
                    let n = payload.len();
                    println!("[{}] ← datagram {n}B — эхо назад", now_hms());
                    let _ = conn_dg.send_datagram(payload);
                }
                Err(_) => break,
            }
        }
    });

    // Bidi-стримы: читаем, эхо назад.
    let bi_task = tokio::spawn(async move {
        loop {
            let (send, recv) = match conn_bi.accept_bi().await {
                Ok(s) => s,
                Err(_) => break,
            };
            tokio::spawn(handle_bi_stream(send, recv));
        }
    });

    tokio::select! {
        _ = dg_task => {}
        _ = bi_task => {}
    }

    println!("[{}] клиент отключён", now_hms());
}

async fn handle_bi_stream(
    mut send: wtransport::SendStream,
    mut recv: wtransport::RecvStream,
) {
    let mut all = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match recv.read(&mut buf).await {
            Ok(Some(n)) => all.extend_from_slice(&buf[..n]),
            Ok(None) => break,
            Err(_) => return,
        }
    }
    println!(
        "[{}] ← stream {}B — эхо назад",
        now_hms(),
        all.len(),
    );
    let _ = send.write_all(&all).await;
    let _ = send.finish().await;
}

fn now_hms() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let s = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    format!(
        "{:02}:{:02}:{:02}",
        (s / 3600) % 24,
        (s / 60) % 60,
        s % 60,
    )
}