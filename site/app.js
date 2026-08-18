const endpoints = [
  { method: "PUT", path: "/models/{model}", description: "Register and lazily load a model bundle from a URI.", category: "lifecycle" },
  { method: "GET", path: "/models", description: "List the model names currently registered by the service.", category: "lifecycle" },
  { method: "GET", path: "/models/{model}", description: "Read bundle origin, metadata, and input/output schemas.", category: "lifecycle" },
  { method: "DELETE", path: "/models/{model}", description: "Evict a model and invalidate its cached transformer.", category: "lifecycle" },
  { method: "POST", path: "/models/{model}/transform", description: "Transform a frame, select output fields, and optionally rank top-k rows.", category: "inference" },
  { method: "POST", path: "/models/{model}/rank", description: "Transform and rank IDs with optional grouping and score averaging.", category: "inference" },
  { method: "GET", path: "/models/{model}/sample", description: "Generate a frame that conforms to the model's expected input schema.", category: "inspect" },
  { method: "GET", path: "/models/{model}/graph", description: "Render the loaded transformer's computation graph as SVG.", category: "inspect" },
  { method: "GET", path: "/models/{model}/health", description: "Run generated sample frames through one loaded model.", category: "inspect" },
  { method: "GET", path: "/health", description: "Check that the model registry and service are responsive.", category: "inspect" },
];

const examples = {
  load: {
    label: "PUT /models/recommender",
    code: `curl -X PUT \\
  http://localhost:8080/models/recommender \\
  --header 'Content-Type: text/plain' \\
  --data 's3://models/production/recommender.zip'`,
  },
  transform: {
    label: "POST /models/recommender/transform",
    code: `curl -X POST \\
  'http://localhost:8080/models/recommender/transform?select=prediction&exec=par-8&missing=error' \\
  --header 'Content-Type: application/json' \\
  --data '{
    "schema": {
      "fields": [
        { "name": "feature_a", "type": "double" },
        { "name": "feature_b", "type": "double" }
      ]
    },
    "rows": [[0.42, 18.7], [0.81, 11.3]]
  }'`,
  },
  rank: {
    label: "POST /models/recommender/rank",
    code: `curl -X POST \\
  'http://localhost:8080/models/recommender/rank?id=item_id&rank=prediction&k=20&desc=true&exec=par-8' \\
  --header 'Content-Type: application/json' \\
  --data @frame.json`,
  },
};

const endpointList = document.querySelector("#endpoint-list");

function renderEndpoints(filter = "all") {
  endpointList.innerHTML = endpoints
    .filter((endpoint) => filter === "all" || endpoint.category === filter)
    .map(
      (endpoint) => `
        <article class="endpoint-row" data-category="${endpoint.category}">
          <span class="endpoint-method ${endpoint.method.toLowerCase()}">${endpoint.method}</span>
          <code class="endpoint-path">${endpoint.path}</code>
          <span class="endpoint-desc">${endpoint.description}</span>
          <span class="endpoint-category">${endpoint.category}</span>
        </article>`,
    )
    .join("");
}

renderEndpoints();

document.querySelectorAll(".api-filter").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll(".api-filter").forEach((item) => item.classList.remove("active"));
    button.classList.add("active");
    renderEndpoints(button.dataset.filter);
  });
});

const requestCode = document.querySelector("#request-code");
const requestLabel = document.querySelector("#request-label");

function setExample(name) {
  const example = examples[name];
  requestCode.textContent = example.code;
  requestLabel.textContent = example.label;
}

setExample("load");

document.querySelectorAll("[data-example]").forEach((button) => {
  button.addEventListener("click", () => {
    document.querySelectorAll("[data-example]").forEach((item) => {
      item.classList.remove("active");
      item.setAttribute("aria-selected", "false");
    });
    button.classList.add("active");
    button.setAttribute("aria-selected", "true");
    setExample(button.dataset.example);
  });
});

async function copyValue(button, value) {
  const original = button.textContent;
  try {
    await navigator.clipboard.writeText(value);
    button.textContent = "Copied";
  } catch {
    button.textContent = "Select";
  }
  window.setTimeout(() => {
    button.textContent = original;
  }, 1400);
}

document.querySelectorAll(".copy-button").forEach((button) => {
  button.addEventListener("click", () => {
    const target = button.dataset.copyTarget;
    const value = target ? document.querySelector(`#${target}`).textContent : button.dataset.copyValue;
    copyValue(button, value);
  });
});

const menuButton = document.querySelector(".menu-toggle");
const siteNav = document.querySelector(".site-nav");

menuButton.addEventListener("click", () => {
  const open = siteNav.classList.toggle("open");
  menuButton.setAttribute("aria-expanded", String(open));
});

siteNav.querySelectorAll("a").forEach((link) => {
  link.addEventListener("click", () => {
    siteNav.classList.remove("open");
    menuButton.setAttribute("aria-expanded", "false");
  });
});

const revealObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        entry.target.classList.add("visible");
        revealObserver.unobserve(entry.target);
      }
    });
  },
  { threshold: 0.08 },
);

document.querySelectorAll(".reveal").forEach((element) => revealObserver.observe(element));

const progress = document.querySelector(".reading-progress span");
window.addEventListener(
  "scroll",
  () => {
    const scrollable = document.documentElement.scrollHeight - window.innerHeight;
    const ratio = scrollable > 0 ? window.scrollY / scrollable : 0;
    progress.style.transform = `scaleX(${Math.min(1, Math.max(0, ratio))})`;
  },
  { passive: true },
);

document.querySelector("#year").textContent = `© ${new Date().getFullYear()} ADRIEL CASELLAS`;
