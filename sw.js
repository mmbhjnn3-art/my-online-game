"use strict";
self.addEventListener("push", event => {
  let data = {};
  try { data = event.data ? event.data.json() : {}; }
  catch (_) { data = { body: event.data ? event.data.text() : "" }; }
  const title = String(data.title || "زندگی مجازی").slice(0, 80);
  const body = String(data.body || "یک پیام جدید برای تو رسیده است.").slice(0, 240);
  const url = (typeof data.url === "string" && data.url.startsWith("/")) ? data.url : "/";
  event.waitUntil(self.registration.showNotification(title, {
    body,
    tag: "zendegi-game-notification",
    data: { url }
  }));
});
self.addEventListener("notificationclick", event => {
  event.notification.close();
  const target = new URL(event.notification.data && event.notification.data.url || "/", self.location.origin).href;
  event.waitUntil((async () => {
    const clientsList = await self.clients.matchAll({ type: "window", includeUncontrolled: true });
    for (const client of clientsList) {
      if (client.url.startsWith(self.location.origin) && "focus" in client) {
        await client.focus();
        if ("navigate" in client) await client.navigate(target);
        return;
      }
    }
    if (self.clients.openWindow) await self.clients.openWindow(target);
  })());
});
