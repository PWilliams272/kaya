// Substring match anywhere in the label (not just prefix, unlike native
// <select> keyboard typeahead) so e.g. "cliffs" finds "Touchstone Cliffs of Id".
function comboFilteredOptions(options, query, excludeValues) {
  const normalizedQuery = query.trim().toLowerCase();
  return options.filter((option) => (
    (!excludeValues || !excludeValues.includes(option.value))
    && option.label.toLowerCase().includes(normalizedQuery)
  ));
}

function renderComboOptionsList(container, options, onPick, emptyText) {
  container.innerHTML = '';
  if (!options.length) {
    const empty = document.createElement('div');
    empty.className = 'combo-option-empty';
    empty.textContent = emptyText;
    container.appendChild(empty);
    return;
  }
  options.forEach((option) => {
    const item = document.createElement('div');
    item.className = 'combo-option';
    item.textContent = option.label;
    // mousedown (not click), preventDefault: fires before the input's blur,
    // so picking an option doesn't first close the panel out from under it.
    item.addEventListener('mousedown', (event) => {
      event.preventDefault();
      onPick(option);
    });
    container.appendChild(item);
  });
}

function bindComboOutsideClose(root, panel, onClose) {
  document.addEventListener('click', (event) => {
    if (!root.contains(event.target)) {
      onClose();
    }
  });
}

function mountSearchableMultiSelect(rootId, options, selectedValues, onChange, placeholder) {
  const root = document.getElementById(rootId);
  const control = root.querySelector('.combo-control');
  const pillsContainer = root.querySelector('.combo-pills');
  const input = root.querySelector('.combo-input');
  const panel = root.querySelector('.combo-panel');
  const optionsNode = root.querySelector('.combo-options');
  const state = {
    options,
    selectedValues: [...selectedValues],
  };

  function renderPills() {
    pillsContainer.innerHTML = '';
    state.selectedValues.forEach((value) => {
      const pill = document.createElement('span');
      pill.className = 'combo-pill';
      const text = document.createElement('span');
      text.textContent = gymName(value);
      const remove = document.createElement('button');
      remove.type = 'button';
      remove.className = 'combo-pill-remove';
      remove.setAttribute('aria-label', `Remove ${gymName(value)}`);
      remove.textContent = '×';
      remove.addEventListener('mousedown', (event) => {
        event.preventDefault();
        event.stopPropagation();
        state.selectedValues = state.selectedValues.filter((existing) => existing !== value);
        renderPills();
        renderOptions();
        onChange([...state.selectedValues]);
      });
      pill.appendChild(text);
      pill.appendChild(remove);
      pillsContainer.appendChild(pill);
    });
    input.placeholder = placeholder;
  }

  function renderOptions() {
    const filtered = comboFilteredOptions(state.options, input.value, state.selectedValues);
    renderComboOptionsList(optionsNode, filtered, (option) => {
      state.selectedValues = [...new Set([...state.selectedValues, option.value])];
      input.value = '';
      renderPills();
      renderOptions();
      onChange([...state.selectedValues]);
      input.focus();
    }, 'No gyms match your search.');
  }

  function open() {
    panel.hidden = false;
    root.classList.add('is-open');
    renderOptions();
  }
  function close() {
    panel.hidden = true;
    root.classList.remove('is-open');
  }

  input.addEventListener('focus', open);
  input.addEventListener('input', renderOptions);
  control.addEventListener('mousedown', (event) => {
    if (event.target === control || event.target === pillsContainer) {
      input.focus();
    }
  });
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      input.blur();
      close();
    } else if (event.key === 'Backspace' && !input.value && state.selectedValues.length) {
      state.selectedValues = state.selectedValues.slice(0, -1);
      renderPills();
      renderOptions();
      onChange([...state.selectedValues]);
    }
  });
  bindComboOutsideClose(root, panel, close);

  appState.widgets[rootId] = {
    update(nextOptions, nextSelectedValues) {
      state.options = nextOptions;
      state.selectedValues = [...nextSelectedValues];
      renderPills();
      renderOptions();
    },
  };

  appState.widgets[rootId].update(options, selectedValues);
}

function mountSearchableSingleSelect(rootId, options, selectedValue, onChange, placeholder) {
  const root = document.getElementById(rootId);
  const input = root.querySelector('.combo-input');
  const panel = root.querySelector('.combo-panel');
  const optionsNode = root.querySelector('.combo-options');
  const state = {
    options,
    selectedValue,
  };

  function currentLabel() {
    const match = state.options.find((option) => option.value === state.selectedValue);
    return match ? match.label : '';
  }

  function syncDisplay() {
    input.value = currentLabel();
  }

  function renderOptions() {
    const filtered = comboFilteredOptions(state.options, input.value, null);
    renderComboOptionsList(optionsNode, filtered, (option) => {
      state.selectedValue = option.value;
      input.value = option.label;
      close();
      onChange(option.value);
    }, 'No gyms match your search.');
  }

  function open() {
    panel.hidden = false;
    root.classList.add('is-open');
    input.value = '';
    renderOptions();
  }
  function close() {
    panel.hidden = true;
    root.classList.remove('is-open');
    syncDisplay();
  }

  input.placeholder = placeholder;
  input.addEventListener('focus', open);
  input.addEventListener('input', renderOptions);
  input.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      input.blur();
      close();
    }
  });
  bindComboOutsideClose(root, panel, close);

  appState.widgets[rootId] = {
    update(nextOptions, nextSelectedValue) {
      state.options = nextOptions;
      state.selectedValue = nextSelectedValue;
      syncDisplay();
    },
  };

  appState.widgets[rootId].update(options, selectedValue);
}

