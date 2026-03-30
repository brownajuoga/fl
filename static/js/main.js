document.addEventListener("DOMContentLoaded", () => {
  const setReturnTo = (form) => {
    if (!form || (form.method || "").toLowerCase() !== "post") {
      return;
    }
    let input = form.querySelector('input[name="return_to"]');
    if (!input) {
      input = document.createElement("input");
      input.type = "hidden";
      input.name = "return_to";
      form.appendChild(input);
    }
    const scopedAnchor = form.closest("details[id], article[id], section[id]");
    const hash = scopedAnchor ? `#${scopedAnchor.id}` : window.location.hash;
    input.value = `${window.location.pathname}${window.location.search}${hash || ""}`;
  };

  const gameType = document.querySelector("[data-game-type]");
  const doubleOnly = document.querySelectorAll(".double-only");

  const bindPageBehavior = () => {
    const currentGameType = document.querySelector("[data-game-type]");
    const currentDoubleOnly = document.querySelectorAll(".double-only");

    const syncDoubleFields = () => {
      const isDoubles = currentGameType && currentGameType.value === "doubles";
      currentDoubleOnly.forEach((field) => {
        field.style.display = isDoubles ? "grid" : "none";
        const select = field.querySelector("select");
        if (!select) {
          return;
        }
        select.required = isDoubles;
        if (!isDoubles) {
          select.value = "";
        }
      });
    };

    if (currentGameType) {
      syncDoubleFields();
      currentGameType.addEventListener("change", syncDoubleFields);
    }

    document.querySelectorAll("[data-cancel-game]").forEach((button) => {
      button.addEventListener("click", (event) => {
        if (!window.confirm("Cancel this game?")) {
          event.preventDefault();
        }
      });
    });

    document.querySelectorAll("[data-delete-entity]").forEach((button) => {
      button.addEventListener("click", (event) => {
        const entity = button.getAttribute("data-delete-entity") || "item";
        if (!window.confirm(`Delete this ${entity}? This cannot be undone.`)) {
          event.preventDefault();
        }
      });
    });

    document.querySelectorAll("form").forEach((form) => {
      form.addEventListener("submit", async (event) => {
        setReturnTo(form);

        if (window.location.pathname !== "/admin" || (form.method || "").toLowerCase() !== "post") {
          return;
        }

        event.preventDefault();
        const scrollY = window.scrollY;
        const openIds = Array.from(document.querySelectorAll("details[open][id]")).map((detail) => detail.id);
        const currentDetail = form.closest("details[id]");
        const detailToClose = currentDetail && currentDetail.id !== "manage-games" ? currentDetail.id : "";
        const formData = new FormData(form);
        const body = new URLSearchParams();
        formData.forEach((value, key) => {
          if (typeof value === "string") {
            body.append(key, value);
          }
        });
        const response = await fetch(form.action, {
          method: "POST",
          body,
          headers: {
            "X-Requested-With": "fetch",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
          },
          credentials: "same-origin",
        });

        if (!response.ok) {
          window.location.href = response.url || window.location.href;
          return;
        }

        const html = await response.text();
        const parser = new DOMParser();
        const doc = parser.parseFromString(html, "text/html");
        const newMain = doc.querySelector("main.site-main");
        const currentMain = document.querySelector("main.site-main");
        if (!newMain || !currentMain) {
          window.location.reload();
          return;
        }
        currentMain.innerHTML = newMain.innerHTML;
        openIds.forEach((id) => {
          if (id === detailToClose) {
            return;
          }
          document.getElementById(id)?.setAttribute("open", "open");
        });
        window.scrollTo(0, scrollY);
        bindPageBehavior();
      });
    });
  };

  bindPageBehavior();
});
