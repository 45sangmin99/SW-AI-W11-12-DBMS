import { h, FunctionComponent, useEffect, useState } from "/engine/component.js";


function OrderHistory({ orders }) {
  return h("section", { class: "history" },
    h("h2", {}, "확정된 주문"),
    h("ul", {}, ...(orders.length
      ? orders.map(id => h("li", { key: id }, `주문 #${id}`))
      : [h("li", { key: "empty" }, "아직 주문이 없어요.")])));
}

function Drop() {
  const [view, setView] = useState({ snapshot: null, message: "드롭을 불러오는 중…", busy: true });
  const { snapshot, message, busy, pendingId, uncertain } = view;

  async function refresh(current = {}) {
    setView(previous => ({ ...previous, busy: true }));
    try {
      const response = await fetch("/api/drop");
      if (!response.ok) throw new Error("Read failed");
      const snapshot = await response.json();
      const confirmed = current.pendingId && snapshot.orders.includes(current.pendingId);
      setView({
        snapshot,
        message: confirmed ? `주문 #${current.pendingId} 확정 · 이력에서 확인됨` : current.message || "",
        pendingId: confirmed ? undefined : current.pendingId,
        uncertain: Boolean(current.uncertain && !confirmed),
        busy: false,
      });
    } catch {
      setView(previous => ({ ...previous, ...current, busy: false, stale: true,
        message: [current.message, "재고를 확인할 수 없어요. 다시 조회해 주세요."].filter(Boolean).join(" ") }));
    }
  }

  async function order() {
    setView(previous => ({ ...previous, busy: true, message: "주문 처리 중…" }));
    let result;
    try {
      const response = await fetch("/api/order", { method: "POST", headers: { "X-Drop-Request": "1" } });
      if (!response.ok) throw new Error("Order response unavailable");
      result = await response.json();
    } catch {
      result = { status: "unknown" };
    }

    const messages = {
      durable: `주문 #${result.orderId} 확정${result.recoveryRequired ? " · DBMS 복구 필요" : ""}`,
      unknown: "주문 결과 확인 필요 · 주문 이력을 다시 조회해 주세요.",
      conflict: "다른 주문이 먼저 확정됐어요.",
      sold_out: "이번 드롭은 품절됐어요.",
      failed: result.recoveryRequired ? "주문 미확정 · DBMS 복구 필요" : "주문을 확정하지 못했어요.",
    };
    const outcome = { message: messages[result.status], pendingId: result.orderId };
    if (result.status === "unknown") {
      setView(previous => ({ ...previous, ...outcome, busy: false, stale: true, uncertain: true }));
      return;
    }
    await refresh({ message: outcome.message });
  }

  useEffect(() => { void refresh(); }, []);

  const canOrder = snapshot && snapshot.stock !== "0" && !view.stale && !busy && !view.uncertain;
  return h("div", {},
    h("p", { class: "stock" }, snapshot ? `남은 수량 ${snapshot.stock} / 3` : "남은 수량 확인 중"),
    h("button", { class: "order", disabled: !canOrder, onClick: order },
      snapshot?.stock === "0" ? "품절" : "한 장 주문하기"),
    h("button", { class: "refresh", disabled: busy, onClick: () => refresh({ message, pendingId,
      uncertain }) }, "재고 및 주문 다시 조회"),
    h("p", { class: "status", role: "status" }, message),
    OrderHistory({ orders: snapshot?.orders ?? [] }));
}

new FunctionComponent(Drop, document.querySelector("#drop")).mount();
