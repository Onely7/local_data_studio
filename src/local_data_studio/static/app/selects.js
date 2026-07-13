const SELECT_CONTROLS = new WeakMap();
const OPEN_CLASS = "is-open";

function labelsFor(select) {
  if (!select.id) return [];
  return [...document.querySelectorAll("label")].filter(
    (label) => label.htmlFor === select.id,
  );
}

function closeOtherSelects(except) {
  document.querySelectorAll(`.select-control.${OPEN_CLASS}`).forEach((control) => {
    if (control !== except) closeSelectControl(control);
  });
}

function closeSelectControl(control) {
  const wrapper = control.wrapper || control;
  const trigger = control.trigger || wrapper.querySelector(".select-trigger");
  wrapper.classList.remove(OPEN_CLASS);
  trigger?.setAttribute("aria-expanded", "false");
  // Option list updates can be queued by the application in the same event turn.
  // Reassert the closed state after those updates without interrupting a new open.
  requestAnimationFrame(() => {
    if (!wrapper.classList.contains(OPEN_CLASS)) {
      trigger?.setAttribute("aria-expanded", "false");
    }
  });
}

function updateOverflowIndicator(control) {
  const remaining =
    control.options.scrollHeight -
    control.options.scrollTop -
    control.options.clientHeight;
  control.popover.classList.toggle("has-more-options", remaining > 1);
}

function optionButton(option, select) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = "select-option";
  button.dataset.value = option.value;
  button.role = "option";
  button.textContent = option.textContent || option.value;
  button.disabled = option.disabled;
  button.setAttribute("aria-selected", String(option.selected));
  if (option.title) button.title = option.title;
  button.addEventListener("click", () => {
    if (option.disabled) return;
    select.value = option.value;
    select.dispatchEvent(new Event("change", { bubbles: true }));
    syncSelectControl(select);
    closeSelectControl(SELECT_CONTROLS.get(select));
  });
  return button;
}

function syncSelectControl(select) {
  const control = SELECT_CONTROLS.get(select);
  if (!control) return;
  const selected = select.selectedOptions[0];
  control.trigger.disabled = select.disabled;
  control.triggerText.textContent = selected?.textContent || "Select option";
  control.options.replaceChildren(
    ...[...select.options].map((option) => optionButton(option, select)),
  );
  const selectClasses = [...select.classList].filter(
    (className) => className !== "select-native",
  );
  control.wrapper.className = `select-control ${selectClasses.join(" ")}`.trim();
  control.trigger.setAttribute("aria-expanded", "false");
  control.wrapper.dataset.selectId = select.id;
  requestAnimationFrame(() => updateOverflowIndicator(control));
}

function openSelectControl(control) {
  if (control.trigger.disabled) return;
  closeOtherSelects(control.wrapper);
  control.wrapper.classList.add(OPEN_CLASS);
  control.trigger.setAttribute("aria-expanded", "true");
  const selected = control.options.querySelector('[aria-selected="true"]');
  selected?.scrollIntoView({ block: "nearest" });
  requestAnimationFrame(() => updateOverflowIndicator(control));
}

function enabledOptions(control) {
  return [...control.options.querySelectorAll(".select-option:not(:disabled)")];
}

function focusOption(control, offset) {
  const options = enabledOptions(control);
  if (!options.length) return;
  const currentIndex = options.findIndex(
    (option) => option.dataset.value === control.select.value,
  );
  const nextIndex = Math.max(0, Math.min(options.length - 1, currentIndex + offset));
  options[nextIndex].focus();
}

function bindKeyboard(control) {
  control.trigger.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeSelectControl(control);
      return;
    }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (!control.wrapper.classList.contains(OPEN_CLASS)) {
        openSelectControl(control);
      }
      focusOption(control, event.key === "ArrowDown" ? 1 : -1);
      return;
    }
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      if (control.wrapper.classList.contains(OPEN_CLASS)) {
        const selected = control.options.querySelector('[aria-selected="true"]');
        selected?.click();
      } else {
        openSelectControl(control);
      }
    }
  });

  control.options.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      closeSelectControl(control);
      control.trigger.focus();
      return;
    }
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const options = enabledOptions(control);
      const currentIndex = options.indexOf(document.activeElement);
      const nextIndex = Math.max(
        0,
        Math.min(options.length - 1, currentIndex + (event.key === "ArrowDown" ? 1 : -1)),
      );
      options[nextIndex]?.focus();
    }
  });
}

function createSelectControl(select) {
  const wrapper = document.createElement("div");
  const trigger = document.createElement("button");
  const triggerText = document.createElement("span");
  const caret = document.createElement("span");
  const popover = document.createElement("div");
  const options = document.createElement("div");
  const controlId = `${select.id || "select"}-control`;

  wrapper.className = "select-control";
  wrapper.dataset.selectId = select.id;
  trigger.type = "button";
  trigger.className = "select-trigger";
  trigger.id = controlId;
  trigger.setAttribute("aria-haspopup", "listbox");
  trigger.setAttribute("aria-expanded", "false");
  triggerText.className = "select-trigger-text";
  caret.className = "select-caret";
  caret.setAttribute("aria-hidden", "true");
  trigger.append(triggerText, caret);
  popover.className = "select-popover";
  options.className = "select-options";
  options.setAttribute("role", "listbox");
  options.setAttribute("aria-labelledby", controlId);
  popover.appendChild(options);

  select.before(wrapper);
  wrapper.append(select, trigger, popover);
  select.classList.add("select-native");
  select.tabIndex = -1;
  select.setAttribute("aria-hidden", "true");
  labelsFor(select).forEach((label) => {
    label.htmlFor = controlId;
  });

  const control = { wrapper, select, trigger, triggerText, popover, options };
  SELECT_CONTROLS.set(select, control);
  trigger.addEventListener("click", () => {
    if (wrapper.classList.contains(OPEN_CLASS)) {
      closeSelectControl(control);
    } else {
      openSelectControl(control);
    }
  });
  select.addEventListener("change", () => syncSelectControl(select));
  options.addEventListener("scroll", () => updateOverflowIndicator(control), {
    passive: true,
  });
  bindKeyboard(control);
  new MutationObserver(() => queueMicrotask(() => syncSelectControl(select))).observe(select, {
    attributes: true,
    childList: true,
    subtree: true,
  });
  syncSelectControl(select);
}

/**
 * Replace native select presentation while preserving select values and change events.
 *
 * The native elements remain the source of truth for existing application code. The
 * enhanced controls only render the options and dispatch the same `change` event.
 */
export function enhanceSelectControls(root = document) {
  root.querySelectorAll("select:not(.select-native)").forEach(createSelectControl);
}

/**
 * Refresh one enhanced select after application code changes its options or value.
 */
export function refreshSelectControl(select) {
  syncSelectControl(select);
}

document.addEventListener("pointerdown", (event) => {
  if (!event.target.closest(".select-control")) closeOtherSelects(null);
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape") closeOtherSelects(null);
});
