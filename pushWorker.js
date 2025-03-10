// pushWorkerRealTime.js

// 1. Carica le variabili d'ambiente dal file .env
require('dotenv').config();

// 2. Importa i moduli necessari
const { Client } = require('pg');
const admin = require('firebase-admin');
const path = require('path');

// 3. Inizializza Firebase Admin usando il file firebase-admin.json
const serviceAccount = require(path.join(__dirname, 'firebase-admin.json'));
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}
console.log("✅ Firebase connesso con successo!");

// 4. Connetti al database usando la variabile DATABASE_URL
const connectionString = process.env.DATABASE_URL;
const pgClient = new Client({ connectionString });

pgClient.connect()
  .then(() => {
    console.log("Connesso al database!");
    // Verifica il database e schema corrente per conferma
    return pgClient.query('SELECT current_database() AS db, current_schema() AS schema;');
  })
  .then(res => {
    console.log("Worker DB/Schema:", res.rows[0]);
    // 5. Metti in ascolto il canale "push_notification_channel"
    return pgClient.query('LISTEN push_notification_channel');
  })
  .then(() => {
    console.log("In ascolto sul canale 'push_notification_channel'");
    // 6. Invia un NOTIFY di test dal worker dopo 5 secondi per verificare l'ascolto
    setTimeout(() => {
      pgClient.query("NOTIFY push_notification_channel, 'test:9876'")
        .then(() => console.log("Test NOTIFY inviato dal worker"))
        .catch((err) => console.error("Errore nell'invio del test NOTIFY:", err));
    }, 5000);
  })
  .catch((err) => {
    console.error("Errore di connessione al DB:", err);
  });

// Gestione degli errori della connessione
pgClient.on('error', (err) => {
  console.error("Errore sul client PostgreSQL:", err);
});

// 7. Quando il worker riceve una notifica, gestiscila
pgClient.on('notification', async (msg) => {
  console.log("Notifica ricevuta:", msg.payload);

  // Gestione per test automatico
  if (msg.payload.startsWith("test:")) {
    console.log("Test NOTIFY ricevuto correttamente.");
    return;
  }
  
  // Gestione per test manuale: se il payload inizia con "manual:"
  if (msg.payload.startsWith("manual:")) {
    console.log("Manual NOTIFY ricevuto:", msg.payload);
    return;
  }
  
  // Gestione per evento reale (ad es. "sold:<announcement_id>")
  if (msg.payload.startsWith("sold:")) {
    const [event, announcementId] = msg.payload.split(':');
    console.log(`Evento ${event} rilevato per annuncio ${announcementId}`);
    
    // Qui puoi inserire la logica per inviare notifiche push ai vari utenti:
    try {
      // Esempio: recupera il venditore dall'annuncio
      const res = await pgClient.query(
        `SELECT user_id FROM announcements WHERE id = $1`, [announcementId]
      );
      if (res.rows.length === 0) {
        console.error("Annuncio non trovato:", announcementId);
        return;
      }
      const sellerId = res.rows[0].user_id;
      console.log(`Venditore recuperato: ${sellerId}`);
      
      // Invia push notification al venditore
      await sendPushNotificationForUser(sellerId, 'Il tuo annuncio è stato acquistato subito!');
      
      // Qui puoi aggiungere ulteriori logiche per miglior offerente o utenti preferiti
    } catch (error) {
      console.error("Errore nel processare l'evento sold:", error);
    }
  }
});

// 8. Funzione per inviare una notifica push a un utente
async function sendPushNotificationForUser(userId, messageBody) {
  try {
    // Recupera il device token dall'utente
    const res = await pgClient.query(
      `SELECT device_token FROM users WHERE id = $1`, [userId]
    );
    const deviceToken = res.rows[0]?.device_token;
    if (!deviceToken) {
      console.log(`Nessun device token per l'utente ${userId}`);
      return;
    }
    // Prepara il messaggio
    const message = {
      notification: {
        title: 'Notifica da BimboSwap',
        body: messageBody,
      },
      token: deviceToken,
    };
    // Invia la notifica push tramite Firebase Admin
    const response = await admin.messaging().send(message);
    console.log(`Push inviata per utente ${userId}:`, response);
  } catch (error) {
    console.error(`Errore nell'invio della push per utente ${userId}:`, error);
  }
}

