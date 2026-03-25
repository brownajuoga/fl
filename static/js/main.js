document.addEventListener("DOMContentLoaded", () => {
  const gameType = document.querySelector("[data-game-type]");
  const doubleOnly = document.querySelectorAll(".double-only");

  const syncDoubleFields = () => {
    const isDoubles = gameType && gameType.value === "doubles";
    doubleOnly.forEach((field) => {
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

  if (gameType) {
    syncDoubleFields();
    gameType.addEventListener("change", syncDoubleFields);
  }

  document.querySelectorAll("[data-cancel-game]").forEach((button) => {
    button.addEventListener("click", (event) => {
      if (!window.confirm("Cancel this game?")) {
        event.preventDefault();
      }
    });
  });
});
