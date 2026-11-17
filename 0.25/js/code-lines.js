// Adds a button to each numbered code block that hides or shows its line numbers.
// Material builds the copy and select buttons into .md-code__nav when a page
// loads, and document$ emits after that, so the new button joins the same row.
document$.subscribe(function () {
  document.querySelectorAll(".highlight").forEach(function (block) {
    var nav = block.querySelector(".md-code__nav");
    if (!block.querySelector(".linenos") || !nav || nav.querySelector("[data-md-type='lines']")) {
      return;
    }
    var button = document.createElement("button");
    button.className = "md-code__button md-code__button--active";
    button.title = "Toggle line numbers";
    button.setAttribute("data-md-type", "lines");
    button.addEventListener("click", function () {
      var hidden = block.classList.toggle("hide-lines");
      button.classList.toggle("md-code__button--active", !hidden);
      button.blur();
    });
    nav.prepend(button);
  });
});
