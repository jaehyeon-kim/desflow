// Opens every link that leaves this documentation site in a new tab.
// A link is external when it points to another host, or to a path on the same host
// outside the site's root, such as the odctl docs or the blog on jaehyeon.me.
(function () {
  var config = document.getElementById("__config");
  var base = config ? JSON.parse(config.textContent).base : ".";
  var root = new URL(base.replace(/\/?$/, "/"), location.href);
  document.querySelectorAll("a[href]").forEach(function (link) {
    var url = new URL(link.getAttribute("href"), location.href);
    if (url.protocol !== "http:" && url.protocol !== "https:") return;
    if (url.host === root.host && url.pathname.indexOf(root.pathname) === 0) return;
    link.target = "_blank";
    link.rel = "noopener";
    if (!link.title) link.title = "Opens in a new tab";
  });
})();
