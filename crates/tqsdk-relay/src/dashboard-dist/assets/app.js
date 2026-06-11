//#region \0vite/modulepreload-polyfill.js
(function polyfill() {
	const relList = document.createElement("link").relList;
	if (relList && relList.supports && relList.supports("modulepreload")) return;
	for (const link of document.querySelectorAll("link[rel=\"modulepreload\"]")) processPreload(link);
	new MutationObserver((mutations) => {
		for (const mutation of mutations) {
			if (mutation.type !== "childList") continue;
			for (const node of mutation.addedNodes) if (node.tagName === "LINK" && node.rel === "modulepreload") processPreload(node);
		}
	}).observe(document, {
		childList: true,
		subtree: true
	});
	function getFetchOpts(link) {
		const fetchOpts = {};
		if (link.integrity) fetchOpts.integrity = link.integrity;
		if (link.referrerPolicy) fetchOpts.referrerPolicy = link.referrerPolicy;
		if (link.crossOrigin === "use-credentials") fetchOpts.credentials = "include";
		else if (link.crossOrigin === "anonymous") fetchOpts.credentials = "omit";
		else fetchOpts.credentials = "same-origin";
		return fetchOpts;
	}
	function processPreload(link) {
		if (link.ep) return;
		link.ep = true;
		const fetchOpts = getFetchOpts(link);
		fetch(link.href, fetchOpts);
	}
})();
var dev_fallback_default = !"production".toLowerCase().startsWith("prod");
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/shared/utils.js
var is_array = Array.isArray;
var index_of = Array.prototype.indexOf;
var includes = Array.prototype.includes;
var array_from = Array.from;
var define_property = Object.defineProperty;
var get_descriptor = Object.getOwnPropertyDescriptor;
var get_descriptors = Object.getOwnPropertyDescriptors;
var object_prototype = Object.prototype;
var array_prototype = Array.prototype;
var get_prototype_of = Object.getPrototypeOf;
var is_extensible = Object.isExtensible;
var noop = () => {};
/** @param {Array<() => void>} arr */
function run_all(arr) {
	for (var i = 0; i < arr.length; i++) arr[i]();
}
/**
* TODO replace with Promise.withResolvers once supported widely enough
* @template [T=void]
*/
function deferred() {
	/** @type {(value: T) => void} */
	var resolve;
	/** @type {(reason: any) => void} */
	var reject;
	return {
		promise: new Promise((res, rej) => {
			resolve = res;
			reject = rej;
		}),
		resolve,
		reject
	};
}
var CLEAN = 1024;
var DIRTY = 2048;
var MAYBE_DIRTY = 4096;
var INERT = 8192;
var DESTROYED = 16384;
/** Set once a reaction has run for the first time */
var REACTION_RAN = 32768;
/** Effect is in the process of getting destroyed. Can be observed in child teardown functions */
var DESTROYING = 1 << 25;
/**
* 'Transparent' effects do not create a transition boundary.
* This is on a block effect 99% of the time but may also be on a branch effect if its parent block effect was pruned
*/
var EFFECT_TRANSPARENT = 65536;
var EFFECT_PRESERVED = 1 << 19;
var USER_EFFECT = 1 << 20;
var EFFECT_OFFSCREEN = 1 << 25;
/**
* Tells that we marked this derived and its reactions as visited during the "mark as (maybe) dirty"-phase.
* Will be lifted during execution of the derived and during checking its dirty state (both are necessary
* because a derived might be checked but not executed). This is a pure performance optimization flag and
* should not be used for any other purpose!
*/
var WAS_MARKED = 65536;
var REACTION_IS_UPDATING = 1 << 21;
var ASYNC = 1 << 22;
var ERROR_VALUE = 1 << 23;
var STATE_SYMBOL = Symbol("$state");
var LEGACY_PROPS = Symbol("legacy props");
var LOADING_ATTR_SYMBOL = Symbol("");
var PROXY_PATH_SYMBOL = Symbol("proxy path");
var ATTRIBUTES_CACHE = Symbol("attributes");
var CLASS_CACHE = Symbol("class");
var STYLE_CACHE = Symbol("style");
var TEXT_CACHE = Symbol("text");
var FORM_RESET_HANDLER = Symbol("form reset");
/** An anchor might change, via this symbol on the original anchor we can tell HMR about the updated anchor */
var HMR_ANCHOR = Symbol("hmr anchor");
/** allow users to ignore aborted signal errors if `reason.name === 'StaleReactionError` */
var STALE_REACTION = new class StaleReactionError extends Error {
	name = "StaleReactionError";
	message = "The reaction that called `getAbortSignal()` was re-run or destroyed";
}();
var IS_XHTML = !!globalThis.document?.contentType && /* @__PURE__ */ globalThis.document.contentType.includes("xml");
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/shared/errors.js
/**
* An invariant violation occurred, meaning Svelte's internal assumptions were flawed. This is a bug in Svelte, not your app — please open an issue at https://github.com/sveltejs/svelte, citing the following message: "%message%"
* @param {string} message
* @returns {never}
*/
function invariant_violation(message) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`invariant_violation\nAn invariant violation occurred, meaning Svelte's internal assumptions were flawed. This is a bug in Svelte, not your app — please open an issue at https://github.com/sveltejs/svelte, citing the following message: "${message}"\nhttps://svelte.dev/e/invariant_violation`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/invariant_violation`);
}
/**
* `%name%(...)` can only be used during component initialisation
* @param {string} name
* @returns {never}
*/
function lifecycle_outside_component(name) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`lifecycle_outside_component\n\`${name}(...)\` can only be used during component initialisation\nhttps://svelte.dev/e/lifecycle_outside_component`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/lifecycle_outside_component`);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/errors.js
/**
* Cannot create a `$derived(...)` with an `await` expression outside of an effect tree
* @returns {never}
*/
function async_derived_orphan() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`async_derived_orphan\nCannot create a \`$derived(...)\` with an \`await\` expression outside of an effect tree\nhttps://svelte.dev/e/async_derived_orphan`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/async_derived_orphan`);
}
/**
* Using `bind:value` together with a checkbox input is not allowed. Use `bind:checked` instead
* @returns {never}
*/
function bind_invalid_checkbox_value() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`bind_invalid_checkbox_value\nUsing \`bind:value\` together with a checkbox input is not allowed. Use \`bind:checked\` instead\nhttps://svelte.dev/e/bind_invalid_checkbox_value`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/bind_invalid_checkbox_value`);
}
/**
* A derived value cannot reference itself recursively
* @returns {never}
*/
function derived_references_self() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`derived_references_self\nA derived value cannot reference itself recursively\nhttps://svelte.dev/e/derived_references_self`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/derived_references_self`);
}
/**
* Keyed each block has duplicate key `%value%` at indexes %a% and %b%
* @param {string} a
* @param {string} b
* @param {string | undefined | null} [value]
* @returns {never}
*/
function each_key_duplicate(a, b, value) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`each_key_duplicate\n${value ? `Keyed each block has duplicate key \`${value}\` at indexes ${a} and ${b}` : `Keyed each block has duplicate key at indexes ${a} and ${b}`}\nhttps://svelte.dev/e/each_key_duplicate`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/each_key_duplicate`);
}
/**
* Keyed each block has key that is not idempotent — the key for item at index %index% was `%a%` but is now `%b%`. Keys must be the same each time for a given item
* @param {string} index
* @param {string} a
* @param {string} b
* @returns {never}
*/
function each_key_volatile(index, a, b) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`each_key_volatile\nKeyed each block has key that is not idempotent — the key for item at index ${index} was \`${a}\` but is now \`${b}\`. Keys must be the same each time for a given item\nhttps://svelte.dev/e/each_key_volatile`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/each_key_volatile`);
}
/**
* `%rune%` cannot be used inside an effect cleanup function
* @param {string} rune
* @returns {never}
*/
function effect_in_teardown(rune) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`effect_in_teardown\n\`${rune}\` cannot be used inside an effect cleanup function\nhttps://svelte.dev/e/effect_in_teardown`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/effect_in_teardown`);
}
/**
* Effect cannot be created inside a `$derived` value that was not itself created inside an effect
* @returns {never}
*/
function effect_in_unowned_derived() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`effect_in_unowned_derived\nEffect cannot be created inside a \`$derived\` value that was not itself created inside an effect\nhttps://svelte.dev/e/effect_in_unowned_derived`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/effect_in_unowned_derived`);
}
/**
* `%rune%` can only be used inside an effect (e.g. during component initialisation)
* @param {string} rune
* @returns {never}
*/
function effect_orphan(rune) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`effect_orphan\n\`${rune}\` can only be used inside an effect (e.g. during component initialisation)\nhttps://svelte.dev/e/effect_orphan`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/effect_orphan`);
}
/**
* Maximum update depth exceeded. This typically indicates that an effect reads and writes the same piece of state
* @returns {never}
*/
function effect_update_depth_exceeded() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`effect_update_depth_exceeded\nMaximum update depth exceeded. This typically indicates that an effect reads and writes the same piece of state\nhttps://svelte.dev/e/effect_update_depth_exceeded`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/effect_update_depth_exceeded`);
}
/**
* Cannot do `bind:%key%={undefined}` when `%key%` has a fallback value
* @param {string} key
* @returns {never}
*/
function props_invalid_value(key) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`props_invalid_value\nCannot do \`bind:${key}={undefined}\` when \`${key}\` has a fallback value\nhttps://svelte.dev/e/props_invalid_value`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/props_invalid_value`);
}
/**
* The `%rune%` rune is only available inside `.svelte` and `.svelte.js/ts` files
* @param {string} rune
* @returns {never}
*/
function rune_outside_svelte(rune) {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`rune_outside_svelte\nThe \`${rune}\` rune is only available inside \`.svelte\` and \`.svelte.js/ts\` files\nhttps://svelte.dev/e/rune_outside_svelte`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/rune_outside_svelte`);
}
/**
* Property descriptors defined on `$state` objects must contain `value` and always be `enumerable`, `configurable` and `writable`.
* @returns {never}
*/
function state_descriptors_fixed() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`state_descriptors_fixed\nProperty descriptors defined on \`$state\` objects must contain \`value\` and always be \`enumerable\`, \`configurable\` and \`writable\`.\nhttps://svelte.dev/e/state_descriptors_fixed`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/state_descriptors_fixed`);
}
/**
* Cannot set prototype of `$state` object
* @returns {never}
*/
function state_prototype_fixed() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`state_prototype_fixed\nCannot set prototype of \`$state\` object\nhttps://svelte.dev/e/state_prototype_fixed`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/state_prototype_fixed`);
}
/**
* Updating state inside `$derived(...)`, `$inspect(...)` or a template expression is forbidden. If the value should not be reactive, declare it without `$state`
* @returns {never}
*/
function state_unsafe_mutation() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`state_unsafe_mutation\nUpdating state inside \`$derived(...)\`, \`$inspect(...)\` or a template expression is forbidden. If the value should not be reactive, declare it without \`$state\`\nhttps://svelte.dev/e/state_unsafe_mutation`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/state_unsafe_mutation`);
}
/**
* A `<svelte:boundary>` `reset` function cannot be called while an error is still being handled
* @returns {never}
*/
function svelte_boundary_reset_onerror() {
	if (dev_fallback_default) {
		const error = /* @__PURE__ */ new Error(`svelte_boundary_reset_onerror\nA \`<svelte:boundary>\` \`reset\` function cannot be called while an error is still being handled\nhttps://svelte.dev/e/svelte_boundary_reset_onerror`);
		error.name = "Svelte error";
		throw error;
	} else throw new Error(`https://svelte.dev/e/svelte_boundary_reset_onerror`);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/constants.js
var HYDRATION_ERROR = {};
var UNINITIALIZED = Symbol("uninitialized");
var FILENAME = Symbol("filename");
var NAMESPACE_HTML = "http://www.w3.org/1999/xhtml";
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/warnings.js
var bold = "font-weight: bold";
var normal = "font-weight: normal";
/**
* Detected reactivity loss when reading `%name%`. This happens when state is read in an async function after an earlier `await`
* @param {string} name
*/
function await_reactivity_loss(name) {
	if (dev_fallback_default) console.warn(`%c[svelte] await_reactivity_loss\n%cDetected reactivity loss when reading \`${name}\`. This happens when state is read in an async function after an earlier \`await\`\nhttps://svelte.dev/e/await_reactivity_loss`, bold, normal);
	else console.warn(`https://svelte.dev/e/await_reactivity_loss`);
}
/**
* An async derived, `%name%` (%location%) was not read immediately after it resolved. This often indicates an unnecessary waterfall, which can slow down your app
* @param {string} name
* @param {string} location
*/
function await_waterfall(name, location) {
	if (dev_fallback_default) console.warn(`%c[svelte] await_waterfall\n%cAn async derived, \`${name}\` (${location}) was not read immediately after it resolved. This often indicates an unnecessary waterfall, which can slow down your app\nhttps://svelte.dev/e/await_waterfall`, bold, normal);
	else console.warn(`https://svelte.dev/e/await_waterfall`);
}
/**
* Reading a derived belonging to a now-destroyed effect may result in stale values
*/
function derived_inert() {
	if (dev_fallback_default) console.warn(`%c[svelte] derived_inert\n%cReading a derived belonging to a now-destroyed effect may result in stale values\nhttps://svelte.dev/e/derived_inert`, bold, normal);
	else console.warn(`https://svelte.dev/e/derived_inert`);
}
/**
* The `%attribute%` attribute on `%html%` changed its value between server and client renders. The client value, `%value%`, will be ignored in favour of the server value
* @param {string} attribute
* @param {string} html
* @param {string} value
*/
function hydration_attribute_changed(attribute, html, value) {
	if (dev_fallback_default) console.warn(`%c[svelte] hydration_attribute_changed\n%cThe \`${attribute}\` attribute on \`${html}\` changed its value between server and client renders. The client value, \`${value}\`, will be ignored in favour of the server value\nhttps://svelte.dev/e/hydration_attribute_changed`, bold, normal);
	else console.warn(`https://svelte.dev/e/hydration_attribute_changed`);
}
/**
* Hydration failed because the initial UI does not match what was rendered on the server. The error occurred near %location%
* @param {string | undefined | null} [location]
*/
function hydration_mismatch(location) {
	if (dev_fallback_default) console.warn(`%c[svelte] hydration_mismatch\n%c${location ? `Hydration failed because the initial UI does not match what was rendered on the server. The error occurred near ${location}` : "Hydration failed because the initial UI does not match what was rendered on the server"}\nhttps://svelte.dev/e/hydration_mismatch`, bold, normal);
	else console.warn(`https://svelte.dev/e/hydration_mismatch`);
}
/**
* The `value` property of a `<select multiple>` element should be an array, but it received a non-array value. The selection will be kept as is.
*/
function select_multiple_invalid_value() {
	if (dev_fallback_default) console.warn(`%c[svelte] select_multiple_invalid_value\n%cThe \`value\` property of a \`<select multiple>\` element should be an array, but it received a non-array value. The selection will be kept as is.\nhttps://svelte.dev/e/select_multiple_invalid_value`, bold, normal);
	else console.warn(`https://svelte.dev/e/select_multiple_invalid_value`);
}
/**
* Reactive `$state(...)` proxies and the values they proxy have different identities. Because of this, comparisons with `%operator%` will produce unexpected results
* @param {string} operator
*/
function state_proxy_equality_mismatch(operator) {
	if (dev_fallback_default) console.warn(`%c[svelte] state_proxy_equality_mismatch\n%cReactive \`$state(...)\` proxies and the values they proxy have different identities. Because of this, comparisons with \`${operator}\` will produce unexpected results\nhttps://svelte.dev/e/state_proxy_equality_mismatch`, bold, normal);
	else console.warn(`https://svelte.dev/e/state_proxy_equality_mismatch`);
}
/**
* A `<svelte:boundary>` `reset` function only resets the boundary the first time it is called
*/
function svelte_boundary_reset_noop() {
	if (dev_fallback_default) console.warn(`%c[svelte] svelte_boundary_reset_noop\n%cA \`<svelte:boundary>\` \`reset\` function only resets the boundary the first time it is called\nhttps://svelte.dev/e/svelte_boundary_reset_noop`, bold, normal);
	else console.warn(`https://svelte.dev/e/svelte_boundary_reset_noop`);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/hydration.js
/** @import { TemplateNode } from '#client' */
/**
* Use this variable to guard everything related to hydration code so it can be treeshaken out
* if the user doesn't use the `hydrate` method and these code paths are therefore not needed.
*/
var hydrating = false;
/** @param {boolean} value */
function set_hydrating(value) {
	hydrating = value;
}
/**
* The node that is currently being hydrated. This starts out as the first node inside the opening
* <!--[--> comment, and updates each time a component calls `$.child(...)` or `$.sibling(...)`.
* When entering a block (e.g. `{#if ...}`), `hydrate_node` is the block opening comment; by the
* time we leave the block it is the closing comment, which serves as the block's anchor.
* @type {TemplateNode}
*/
var hydrate_node;
/** @param {TemplateNode | null} node */
function set_hydrate_node(node) {
	if (node === null) {
		hydration_mismatch();
		throw HYDRATION_ERROR;
	}
	return hydrate_node = node;
}
function hydrate_next() {
	return set_hydrate_node(/* @__PURE__ */ get_next_sibling(hydrate_node));
}
/** @param {TemplateNode} node */
function reset(node) {
	if (!hydrating) return;
	if (/* @__PURE__ */ get_next_sibling(hydrate_node) !== null) {
		hydration_mismatch();
		throw HYDRATION_ERROR;
	}
	hydrate_node = node;
}
function next(count = 1) {
	if (hydrating) {
		var i = count;
		var node = hydrate_node;
		while (i--) node = /* @__PURE__ */ get_next_sibling(node);
		hydrate_node = node;
	}
}
/**
* Skips or removes (depending on {@link remove}) all nodes starting at `hydrate_node` up until the next hydration end comment
* @param {boolean} remove
*/
function skip_nodes(remove = true) {
	var depth = 0;
	var node = hydrate_node;
	while (true) {
		if (node.nodeType === 8) {
			var data = node.data;
			if (data === "]") {
				if (depth === 0) return node;
				depth -= 1;
			} else if (data === "[" || data === "[!" || data[0] === "[" && !isNaN(Number(data.slice(1)))) depth += 1;
		}
		var next = /* @__PURE__ */ get_next_sibling(node);
		if (remove) node.remove();
		node = next;
	}
}
/**
*
* @param {TemplateNode} node
*/
function read_hydration_instruction(node) {
	if (!node || node.nodeType !== 8) {
		hydration_mismatch();
		throw HYDRATION_ERROR;
	}
	return node.data;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/equality.js
/** @import { Equals } from '#client' */
/** @type {Equals} */
function equals(value) {
	return value === this.v;
}
/**
* @param {unknown} a
* @param {unknown} b
* @returns {boolean}
*/
function safe_not_equal(a, b) {
	return a != a ? b == b : a !== b || a !== null && typeof a === "object" || typeof a === "function";
}
/** @type {Equals} */
function safe_equals(value) {
	return !safe_not_equal(value, this.v);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/flags/index.js
/** True if experimental.async=true */
var async_mode_flag = false;
/** True if we're not certain that we only have Svelte 5 code in the compilation */
var legacy_mode_flag = false;
/** True if $inspect.trace is used */
var tracing_mode_flag = false;
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dev/tracing.js
/**
* @typedef {{
*   traces: Error[];
* }} TraceEntry
*/
/** @type {{ reaction: Reaction | null, entries: Map<Value, TraceEntry> } | null} */
var tracing_expressions = null;
/**
* @param {Value} source
* @param {string} label
*/
function tag(source, label) {
	source.label = label;
	tag_proxy(source.v, label);
	return source;
}
/**
* @param {unknown} value
* @param {string} label
*/
function tag_proxy(value, label) {
	value?.[PROXY_PATH_SYMBOL]?.(label);
	return value;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/shared/dev.js
/**
* @param {string} label
* @returns {Error & { stack: string } | null}
*/
function get_error(label) {
	const error = /* @__PURE__ */ new Error();
	const stack = get_stack();
	if (stack.length === 0) return null;
	stack.unshift("\n");
	define_property(error, "stack", { value: stack.join("\n") });
	define_property(error, "name", { value: label });
	return error;
}
/**
* @returns {string[]}
*/
function get_stack() {
	const limit = Error.stackTraceLimit;
	Error.stackTraceLimit = Infinity;
	const stack = (/* @__PURE__ */ new Error()).stack;
	Error.stackTraceLimit = limit;
	if (!stack) return [];
	const lines = stack.split("\n");
	const new_lines = [];
	for (let i = 0; i < lines.length; i++) {
		const line = lines[i];
		const posixified = line.replaceAll("\\", "/");
		if (line.trim() === "Error") continue;
		if (line.includes("validate_each_keys")) return [];
		if (posixified.includes("svelte/src/internal") || posixified.includes("node_modules/.vite")) continue;
		new_lines.push(line);
	}
	return new_lines;
}
/**
* @param {boolean} condition
* @param {string} message
*/
function invariant(condition, message) {
	if (!dev_fallback_default) throw new Error("invariant(...) was not guarded by if (DEV)");
	if (!condition) invariant_violation(message);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/context.js
/** @import { ComponentContext, DevStackEntry, Effect } from '#client' */
/** @type {ComponentContext | null} */
var component_context = null;
/** @param {ComponentContext | null} context */
function set_component_context(context) {
	component_context = context;
}
/** @type {DevStackEntry | null} */
var dev_stack = null;
/** @param {DevStackEntry | null} stack */
function set_dev_stack(stack) {
	dev_stack = stack;
}
/**
* The current component function. Different from current component context:
* ```html
* <!-- App.svelte -->
* <Foo>
*   <Bar /> <!-- context == Foo.svelte, function == App.svelte -->
* </Foo>
* ```
* @type {ComponentContext['function']}
*/
var dev_current_component_function = null;
/** @param {ComponentContext['function']} fn */
function set_dev_current_component_function(fn) {
	dev_current_component_function = fn;
}
/**
* @param {Record<string, unknown>} props
* @param {any} runes
* @param {Function} [fn]
* @returns {void}
*/
function push(props, runes = false, fn) {
	component_context = {
		p: component_context,
		i: false,
		c: null,
		e: null,
		s: props,
		x: null,
		r: active_effect,
		l: legacy_mode_flag && !runes ? {
			s: null,
			u: null,
			$: []
		} : null
	};
	if (dev_fallback_default) {
		component_context.function = fn;
		dev_current_component_function = fn;
	}
}
/**
* @template {Record<string, any>} T
* @param {T} [component]
* @returns {T}
*/
function pop(component) {
	var context = component_context;
	var effects = context.e;
	if (effects !== null) {
		context.e = null;
		for (var fn of effects) create_user_effect(fn);
	}
	if (component !== void 0) context.x = component;
	context.i = true;
	component_context = context.p;
	if (dev_fallback_default) dev_current_component_function = component_context?.function ?? null;
	return component ?? {};
}
/** @returns {boolean} */
function is_runes() {
	return !legacy_mode_flag || component_context !== null && component_context.l === null;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/task.js
/** @type {Array<() => void>} */
var micro_tasks = [];
function run_micro_tasks() {
	var tasks = micro_tasks;
	micro_tasks = [];
	run_all(tasks);
}
/**
* @param {() => void} fn
*/
function queue_micro_task(fn) {
	if (micro_tasks.length === 0 && !is_flushing_sync) {
		var tasks = micro_tasks;
		queueMicrotask(() => {
			if (tasks === micro_tasks) run_micro_tasks();
		});
	}
	micro_tasks.push(fn);
}
/**
* Synchronously run any queued tasks.
*/
function flush_tasks() {
	while (micro_tasks.length > 0) run_micro_tasks();
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/error-handling.js
/** @import { Derived, Effect } from '#client' */
/** @import { Boundary } from './dom/blocks/boundary.js' */
var adjustments = /* @__PURE__ */ new WeakMap();
/**
* @param {unknown} error
*/
function handle_error(error) {
	var effect = active_effect;
	if (effect === null) {
		/** @type {Derived} */ active_reaction.f |= ERROR_VALUE;
		return error;
	}
	if (dev_fallback_default && error instanceof Error && !adjustments.has(error)) adjustments.set(error, get_adjustments(error, effect));
	if ((effect.f & 32768) === 0 && (effect.f & 4) === 0) {
		if (dev_fallback_default && !effect.parent && error instanceof Error) apply_adjustments(error);
		throw error;
	}
	invoke_error_boundary(error, effect);
}
/**
* @param {unknown} error
* @param {Effect | null} effect
*/
function invoke_error_boundary(error, effect) {
	if (effect !== null && (effect.f & 16384) !== 0) return;
	while (effect !== null) {
		if ((effect.f & 128) !== 0) {
			if ((effect.f & 32768) === 0) throw error;
			try {
				/** @type {Boundary} */ effect.b.error(error);
				return;
			} catch (e) {
				error = e;
			}
		}
		effect = effect.parent;
	}
	if (dev_fallback_default && error instanceof Error) apply_adjustments(error);
	throw error;
}
/**
* Add useful information to the error message/stack in development
* @param {Error} error
* @param {Effect} effect
*/
function get_adjustments(error, effect) {
	const message_descriptor = get_descriptor(error, "message");
	if (message_descriptor && !message_descriptor.configurable) return;
	var indent = is_firefox ? "  " : "	";
	var component_stack = `\n${indent}in ${effect.fn?.name || "<unknown>"}`;
	var context = effect.ctx;
	while (context !== null) {
		component_stack += `\n${indent}in ${context.function?.[FILENAME].split("/").pop()}`;
		context = context.p;
	}
	return {
		message: error.message + `\n${component_stack}\n`,
		stack: error.stack?.split("\n").filter((line) => !line.includes("svelte/src/internal")).join("\n")
	};
}
/**
* @param {Error} error
*/
function apply_adjustments(error) {
	const adjusted = adjustments.get(error);
	if (adjusted) {
		define_property(error, "message", { value: adjusted.message });
		define_property(error, "stack", { value: adjusted.stack });
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/status.js
/** @import { Derived, Signal } from '#client' */
var STATUS_MASK = ~(DIRTY | MAYBE_DIRTY | CLEAN);
/**
* @param {Signal} signal
* @param {number} status
*/
function set_signal_status(signal, status) {
	signal.f = signal.f & STATUS_MASK | status;
}
/**
* Set a derived's status to CLEAN or MAYBE_DIRTY based on its connection state.
* @param {Derived} derived
*/
function update_derived_status(derived) {
	if ((derived.f & 512) !== 0 || derived.deps === null) set_signal_status(derived, CLEAN);
	else set_signal_status(derived, MAYBE_DIRTY);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/utils.js
/** @import { Derived, Effect, Value } from '#client' */
/**
* @param {Value[] | null} deps
*/
function clear_marked(deps) {
	if (deps === null) return;
	for (const dep of deps) {
		if ((dep.f & 2) === 0 || (dep.f & 65536) === 0) continue;
		dep.f ^= WAS_MARKED;
		clear_marked(
			/** @type {Derived} */
			dep.deps
		);
	}
}
/**
* @param {Effect} effect
* @param {Set<Effect>} dirty_effects
* @param {Set<Effect>} maybe_dirty_effects
*/
function defer_effect(effect, dirty_effects, maybe_dirty_effects) {
	if ((effect.f & 2048) !== 0) dirty_effects.add(effect);
	else if ((effect.f & 4096) !== 0) maybe_dirty_effects.add(effect);
	clear_marked(effect.deps);
	set_signal_status(effect, CLEAN);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/store.js
/**
* We set this to `true` when updating a store so that we correctly
* schedule effects if the update takes place inside a `$:` effect
*/
var legacy_is_updating_store = false;
/**
* Whether or not the prop currently being read is a store binding, as in
* `<Child bind:x={$y} />`. If it is, we treat the prop as mutable even in
* runes mode, and skip `binding_property_non_reactive` validation
*/
var is_store_binding = false;
/**
* Returns a tuple that indicates whether `fn()` reads a prop that is a store binding.
* Used to prevent `binding_property_non_reactive` validation false positives and
* ensure that these props are treated as mutable even in runes mode
* @template T
* @param {() => T} fn
* @returns {[T, boolean]}
*/
function capture_store_binding(fn) {
	var previous_is_store_binding = is_store_binding;
	try {
		is_store_binding = false;
		return [fn(), is_store_binding];
	} finally {
		is_store_binding = previous_is_store_binding;
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/reactivity/create-subscriber.js
/**
* Returns a `subscribe` function that integrates external event-based systems with Svelte's reactivity.
* It's particularly useful for integrating with web APIs like `MediaQuery`, `IntersectionObserver`, or `WebSocket`.
*
* If `subscribe` is called inside an effect (including indirectly, for example inside a getter),
* the `start` callback will be called with an `update` function. Whenever `update` is called, the effect re-runs.
*
* If `start` returns a cleanup function, it will be called when the effect is destroyed.
*
* If `subscribe` is called in multiple effects, `start` will only be called once as long as the effects
* are active, and the returned teardown function will only be called when all effects are destroyed.
*
* It's best understood with an example. Here's an implementation of [`MediaQuery`](https://svelte.dev/docs/svelte/svelte-reactivity#MediaQuery):
*
* ```js
* import { createSubscriber } from 'svelte/reactivity';
* import { on } from 'svelte/events';
*
* export class MediaQuery {
* 	#query;
* 	#subscribe;
*
* 	constructor(query) {
* 		this.#query = window.matchMedia(`(${query})`);
*
* 		this.#subscribe = createSubscriber((update) => {
* 			// when the `change` event occurs, re-run any effects that read `this.current`
* 			const off = on(this.#query, 'change', update);
*
* 			// stop listening when all the effects are destroyed
* 			return () => off();
* 		});
* 	}
*
* 	get current() {
* 		// This makes the getter reactive, if read in an effect
* 		this.#subscribe();
*
* 		// Return the current state of the query, whether or not we're in an effect
* 		return this.#query.matches;
* 	}
* }
* ```
* @param {(update: () => void) => (() => void) | void} start
* @since 5.7.0
*/
function createSubscriber(start) {
	let subscribers = 0;
	let version = source(0);
	/** @type {(() => void) | void} */
	let stop;
	if (dev_fallback_default) tag(version, "createSubscriber version");
	return () => {
		if (effect_tracking()) {
			get(version);
			render_effect(() => {
				if (subscribers === 0) stop = untrack(() => start(() => increment(version)));
				subscribers += 1;
				return () => {
					queue_micro_task(() => {
						subscribers -= 1;
						if (subscribers === 0) {
							stop?.();
							stop = void 0;
							increment(version);
						}
					});
				};
			});
		}
	};
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/blocks/boundary.js
/** @import { Effect, Source, TemplateNode, } from '#client' */
/**
* @typedef {{
* 	 onerror?: ((error: unknown, reset: () => void) => void) | null;
*   failed?: ((anchor: Node, error: () => unknown, reset: () => () => void) => void) | null;
*   pending?: ((anchor: Node) => void) | null;
* }} BoundaryProps
*/
var flags = EFFECT_TRANSPARENT | EFFECT_PRESERVED;
/**
* @param {TemplateNode} node
* @param {BoundaryProps} props
* @param {((anchor: Node) => void)} children
* @param {((error: unknown) => unknown) | undefined} [transform_error]
* @returns {void}
*/
function boundary(node, props, children, transform_error) {
	new Boundary(node, props, children, transform_error);
}
var Boundary = class {
	/** @type {Boundary | null} */
	parent;
	is_pending = false;
	/**
	* API-level transformError transform function. Transforms errors before they reach the `failed` snippet.
	* Inherited from parent boundary, or defaults to identity.
	* @type {(error: unknown) => unknown}
	*/
	transform_error;
	/** @type {TemplateNode} */
	#anchor;
	/** @type {TemplateNode | null} */
	#hydrate_open = hydrating ? hydrate_node : null;
	/** @type {BoundaryProps} */
	#props;
	/** @type {((anchor: Node) => void)} */
	#children;
	/** @type {Effect} */
	#effect;
	/** @type {Effect | null} */
	#main_effect = null;
	/** @type {Effect | null} */
	#pending_effect = null;
	/** @type {Effect | null} */
	#failed_effect = null;
	/** @type {DocumentFragment | null} */
	#offscreen_fragment = null;
	#local_pending_count = 0;
	#pending_count = 0;
	#pending_count_update_queued = false;
	/** @type {Set<Effect>} */
	#dirty_effects = /* @__PURE__ */ new Set();
	/** @type {Set<Effect>} */
	#maybe_dirty_effects = /* @__PURE__ */ new Set();
	/**
	* A source containing the number of pending async deriveds/expressions.
	* Only created if `$effect.pending()` is used inside the boundary,
	* otherwise updating the source results in needless `Batch.ensure()`
	* calls followed by no-op flushes
	* @type {Source<number> | null}
	*/
	#effect_pending = null;
	#effect_pending_subscriber = createSubscriber(() => {
		this.#effect_pending = source(this.#local_pending_count);
		if (dev_fallback_default) tag(this.#effect_pending, "$effect.pending()");
		return () => {
			this.#effect_pending = null;
		};
	});
	/**
	* @param {TemplateNode} node
	* @param {BoundaryProps} props
	* @param {((anchor: Node) => void)} children
	* @param {((error: unknown) => unknown) | undefined} [transform_error]
	*/
	constructor(node, props, children, transform_error) {
		this.#anchor = node;
		this.#props = props;
		this.#children = (anchor) => {
			var effect = active_effect;
			effect.b = this;
			effect.f |= 128;
			children(anchor);
		};
		this.parent = active_effect.b;
		this.transform_error = transform_error ?? this.parent?.transform_error ?? ((e) => e);
		this.#effect = block(() => {
			if (hydrating) {
				const comment = this.#hydrate_open;
				hydrate_next();
				const server_rendered_pending = comment.data === "[!";
				if (comment.data.startsWith("[?")) {
					const serialized_error = JSON.parse(comment.data.slice(2));
					this.#hydrate_failed_content(serialized_error);
				} else if (server_rendered_pending) this.#hydrate_pending_content();
				else this.#hydrate_resolved_content();
			} else this.#render();
		}, flags);
		if (hydrating) this.#anchor = hydrate_node;
	}
	#hydrate_resolved_content() {
		try {
			this.#main_effect = branch(() => this.#children(this.#anchor));
		} catch (error) {
			this.error(error);
		}
	}
	/**
	* @param {unknown} error The deserialized error from the server's hydration comment
	*/
	#hydrate_failed_content(error) {
		const failed = this.#props.failed;
		if (!failed) return;
		this.#failed_effect = branch(() => {
			failed(this.#anchor, () => error, () => () => {});
		});
	}
	#hydrate_pending_content() {
		const pending = this.#props.pending;
		if (!pending) return;
		this.is_pending = true;
		this.#pending_effect = branch(() => pending(this.#anchor));
		queue_micro_task(() => {
			var fragment = this.#offscreen_fragment = document.createDocumentFragment();
			var anchor = create_text();
			fragment.append(anchor);
			this.#main_effect = this.#run(() => {
				return branch(() => this.#children(anchor));
			});
			if (this.#pending_count === 0) {
				this.#anchor.before(fragment);
				this.#offscreen_fragment = null;
				pause_effect(this.#pending_effect, () => {
					this.#pending_effect = null;
				});
				this.#resolve(current_batch);
			}
		});
	}
	#render() {
		try {
			this.is_pending = this.has_pending_snippet();
			this.#pending_count = 0;
			this.#local_pending_count = 0;
			this.#main_effect = branch(() => {
				this.#children(this.#anchor);
			});
			if (this.#pending_count > 0) {
				var fragment = this.#offscreen_fragment = document.createDocumentFragment();
				move_effect(this.#main_effect, fragment);
				const pending = this.#props.pending;
				this.#pending_effect = branch(() => pending(this.#anchor));
			} else this.#resolve(current_batch);
		} catch (error) {
			this.error(error);
		}
	}
	/**
	* @param {Batch} batch
	*/
	#resolve(batch) {
		this.is_pending = false;
		batch.transfer_effects(this.#dirty_effects, this.#maybe_dirty_effects);
	}
	/**
	* Defer an effect inside a pending boundary until the boundary resolves
	* @param {Effect} effect
	*/
	defer_effect(effect) {
		defer_effect(effect, this.#dirty_effects, this.#maybe_dirty_effects);
	}
	/**
	* Returns `false` if the effect exists inside a boundary whose pending snippet is shown
	* @returns {boolean}
	*/
	is_rendered() {
		return !this.is_pending && (!this.parent || this.parent.is_rendered());
	}
	has_pending_snippet() {
		return !!this.#props.pending;
	}
	/**
	* @template T
	* @param {() => T} fn
	*/
	#run(fn) {
		var previous_effect = active_effect;
		var previous_reaction = active_reaction;
		var previous_ctx = component_context;
		set_active_effect(this.#effect);
		set_active_reaction(this.#effect);
		set_component_context(this.#effect.ctx);
		try {
			Batch.ensure();
			return fn();
		} catch (e) {
			handle_error(e);
			return null;
		} finally {
			set_active_effect(previous_effect);
			set_active_reaction(previous_reaction);
			set_component_context(previous_ctx);
		}
	}
	/**
	* Updates the pending count associated with the currently visible pending snippet,
	* if any, such that we can replace the snippet with content once work is done
	* @param {1 | -1} d
	* @param {Batch} batch
	*/
	#update_pending_count(d, batch) {
		if (!this.has_pending_snippet()) {
			if (this.parent) this.parent.#update_pending_count(d, batch);
			return;
		}
		this.#pending_count += d;
		if (this.#pending_count === 0) {
			this.#resolve(batch);
			if (this.#pending_effect) pause_effect(this.#pending_effect, () => {
				this.#pending_effect = null;
			});
			if (this.#offscreen_fragment) {
				this.#anchor.before(this.#offscreen_fragment);
				this.#offscreen_fragment = null;
			}
		}
	}
	/**
	* Update the source that powers `$effect.pending()` inside this boundary,
	* and controls when the current `pending` snippet (if any) is removed.
	* Do not call from inside the class
	* @param {1 | -1} d
	* @param {Batch} batch
	*/
	update_pending_count(d, batch) {
		this.#update_pending_count(d, batch);
		this.#local_pending_count += d;
		if (!this.#effect_pending || this.#pending_count_update_queued) return;
		this.#pending_count_update_queued = true;
		queue_micro_task(() => {
			this.#pending_count_update_queued = false;
			if (this.#effect_pending) internal_set(this.#effect_pending, this.#local_pending_count);
		});
	}
	get_effect_pending() {
		this.#effect_pending_subscriber();
		return get(this.#effect_pending);
	}
	/** @param {unknown} error */
	error(error) {
		if (!this.#props.onerror && !this.#props.failed) throw error;
		if (current_batch?.is_fork) {
			if (this.#main_effect) current_batch.skip_effect(this.#main_effect);
			if (this.#pending_effect) current_batch.skip_effect(this.#pending_effect);
			if (this.#failed_effect) current_batch.skip_effect(this.#failed_effect);
			current_batch.oncommit(() => {
				this.#handle_error(error);
			});
		} else this.#handle_error(error);
	}
	/**
	* @param {unknown} error
	*/
	#handle_error(error) {
		if (this.#main_effect) {
			destroy_effect(this.#main_effect);
			this.#main_effect = null;
		}
		if (this.#pending_effect) {
			destroy_effect(this.#pending_effect);
			this.#pending_effect = null;
		}
		if (this.#failed_effect) {
			destroy_effect(this.#failed_effect);
			this.#failed_effect = null;
		}
		if (hydrating) {
			set_hydrate_node(this.#hydrate_open);
			next();
			set_hydrate_node(skip_nodes());
		}
		var onerror = this.#props.onerror;
		let failed = this.#props.failed;
		var did_reset = false;
		var calling_on_error = false;
		const reset = () => {
			if (did_reset) {
				svelte_boundary_reset_noop();
				return;
			}
			did_reset = true;
			if (calling_on_error) svelte_boundary_reset_onerror();
			if (this.#failed_effect !== null) pause_effect(this.#failed_effect, () => {
				this.#failed_effect = null;
			});
			this.#run(() => {
				this.#render();
			});
		};
		/** @param {unknown} transformed_error */
		const handle_error_result = (transformed_error) => {
			try {
				calling_on_error = true;
				onerror?.(transformed_error, reset);
				calling_on_error = false;
			} catch (error) {
				invoke_error_boundary(error, this.#effect && this.#effect.parent);
			}
			if (failed) this.#failed_effect = this.#run(() => {
				try {
					return branch(() => {
						var effect = active_effect;
						effect.b = this;
						effect.f |= 128;
						failed(this.#anchor, () => transformed_error, () => reset);
					});
				} catch (error) {
					invoke_error_boundary(error, this.#effect.parent);
					return null;
				}
			});
		};
		queue_micro_task(() => {
			/** @type {unknown} */
			var result;
			try {
				result = this.transform_error(error);
			} catch (e) {
				invoke_error_boundary(e, this.#effect && this.#effect.parent);
				return;
			}
			if (result !== null && typeof result === "object" && typeof result.then === "function")
 /** @type {any} */ result.then(
				handle_error_result,
				/** @param {unknown} e */
				(e) => invoke_error_boundary(e, this.#effect && this.#effect.parent)
			);
			else handle_error_result(result);
		});
	}
};
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/async.js
/** @import { Blocker, Effect, Source, Value } from '#client' */
/**
* @param {Blocker[]} blockers
* @param {Array<() => any>} sync
* @param {Array<() => Promise<any>>} async
* @param {(values: Value[]) => any} fn
*/
function flatten(blockers, sync, async, fn) {
	const d = is_runes() ? derived : derived_safe_equal;
	var pending = blockers.filter((b) => !b.settled);
	var deriveds = sync.map(d);
	if (dev_fallback_default) deriveds.forEach((d, i) => {
		d.label = sync[i].toString().replace("() => ", "").replaceAll("$.eager(() => ", "$state.eager(").replace(/\$\.get\((.+?)\)/g, (_, id) => id);
	});
	if (async.length === 0 && pending.length === 0) {
		fn(deriveds);
		return;
	}
	var parent = active_effect;
	var restore = capture();
	var blocker_promise = pending.length === 1 ? pending[0].promise : pending.length > 1 ? Promise.all(pending.map((b) => b.promise)) : null;
	/**
	* @param {Source[]} async
	*/
	function finish(async) {
		if ((parent.f & 16384) !== 0) return;
		restore();
		try {
			fn([...deriveds, ...async]);
		} catch (error) {
			invoke_error_boundary(error, parent);
		}
		unset_context();
	}
	var decrement_pending = increment_pending();
	if (async.length === 0) {
		/** @type {Promise<any>} */ blocker_promise.then(() => finish([])).finally(decrement_pending);
		return;
	}
	function run() {
		Promise.all(async.map((expression) => /* @__PURE__ */ async_derived(expression))).then(finish).catch((error) => invoke_error_boundary(error, parent)).finally(decrement_pending);
	}
	if (blocker_promise) blocker_promise.then(() => {
		restore();
		run();
		unset_context();
	});
	else run();
}
/**
* Captures the current effect context so that we can restore it after
* some asynchronous work has happened (so that e.g. `await a + b`
* causes `b` to be registered as a dependency).
*/
function capture() {
	var previous_effect = active_effect;
	var previous_reaction = active_reaction;
	var previous_component_context = component_context;
	var previous_batch = current_batch;
	if (dev_fallback_default) var previous_dev_stack = dev_stack;
	return function restore(activate_batch = true) {
		set_active_effect(previous_effect);
		set_active_reaction(previous_reaction);
		set_component_context(previous_component_context);
		if (activate_batch && (previous_effect.f & 16384) === 0) {
			previous_batch?.activate();
			previous_batch?.apply();
		}
		if (dev_fallback_default) {
			set_reactivity_loss_tracker(null);
			set_dev_stack(previous_dev_stack);
		}
	};
}
function unset_context(deactivate_batch = true) {
	set_active_effect(null);
	set_active_reaction(null);
	set_component_context(null);
	if (deactivate_batch) current_batch?.deactivate();
	if (dev_fallback_default) {
		set_reactivity_loss_tracker(null);
		set_dev_stack(null);
	}
}
/**
* @returns {(skip?: boolean) => void}
*/
function increment_pending() {
	var effect = active_effect;
	var boundary = effect.b;
	var batch = current_batch;
	var blocking = !!boundary?.is_rendered();
	boundary?.update_pending_count(1, batch);
	batch.increment(blocking, effect);
	return () => {
		boundary?.update_pending_count(-1, batch);
		batch.decrement(blocking, effect);
	};
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/deriveds.js
/** @import { Derived, Effect, Reaction, Source, Value } from '#client' */
/** @import { Batch } from './batch.js'; */
/** @import { Boundary } from '../dom/blocks/boundary.js'; */
/**
* This allows us to track 'reactivity loss' that occurs when signals
* are read after a non-context-restoring `await`. Dev-only
* @type {{ effect: Effect, effect_deps: Set<Value>, warned: boolean } | null}
*/
var reactivity_loss_tracker = null;
/** @param {{ effect: Effect, effect_deps: Set<Value>, warned: boolean } | null} v */
function set_reactivity_loss_tracker(v) {
	reactivity_loss_tracker = v;
}
var recent_async_deriveds = /* @__PURE__ */ new Set();
/**
* @template V
* @param {() => V} fn
* @returns {Derived<V>}
*/
/*#__NO_SIDE_EFFECTS__*/
function derived(fn) {
	var flags = 2 | DIRTY;
	if (active_effect !== null) active_effect.f |= EFFECT_PRESERVED;
	/** @type {Derived<V>} */
	const signal = {
		ctx: component_context,
		deps: null,
		effects: null,
		equals,
		f: flags,
		fn,
		reactions: null,
		rv: 0,
		v: UNINITIALIZED,
		wv: 0,
		parent: active_effect,
		ac: null
	};
	if (dev_fallback_default && tracing_mode_flag) signal.created = get_error("created at");
	return signal;
}
var OBSOLETE = Symbol("obsolete");
/**
* @template V
* @param {() => V | Promise<V>} fn
* @param {string} [label]
* @param {string} [location] If provided, print a warning if the value is not read immediately after update
* @returns {Promise<Source<V>>}
*/
/*#__NO_SIDE_EFFECTS__*/
function async_derived(fn, label, location) {
	let parent = active_effect;
	if (parent === null) async_derived_orphan();
	var promise = void 0;
	var signal = source(UNINITIALIZED);
	if (dev_fallback_default) signal.label = label ?? fn.toString();
	var should_suspend = !active_reaction;
	/** @type {Set<ReturnType<typeof deferred<V>>>} */
	var deferreds = /* @__PURE__ */ new Set();
	async_effect(() => {
		var effect = active_effect;
		if (dev_fallback_default) reactivity_loss_tracker = {
			effect,
			effect_deps: /* @__PURE__ */ new Set(),
			warned: false
		};
		/** @type {ReturnType<typeof deferred<V>>} */
		var d = deferred();
		promise = d.promise;
		try {
			Promise.resolve(fn()).then(d.resolve, (e) => {
				if (e !== STALE_REACTION) d.reject(e);
			}).finally(unset_context);
		} catch (error) {
			d.reject(error);
			unset_context();
		}
		if (dev_fallback_default) {
			if (reactivity_loss_tracker) {
				if (effect.deps !== null) for (let i = 0; i < skipped_deps; i += 1) reactivity_loss_tracker.effect_deps.add(effect.deps[i]);
				if (new_deps !== null) for (let i = 0; i < new_deps.length; i += 1) reactivity_loss_tracker.effect_deps.add(new_deps[i]);
			}
			reactivity_loss_tracker = null;
		}
		var batch = current_batch;
		if (should_suspend) {
			if ((effect.f & 32768) !== 0) var decrement_pending = increment_pending();
			if (parent.b?.is_rendered()) batch.async_deriveds.get(effect)?.reject(OBSOLETE);
			else for (const d of deferreds.values()) d.reject(OBSOLETE);
			deferreds.add(d);
			batch.async_deriveds.set(effect, d);
		}
		/**
		* @param {any} value
		* @param {unknown} error
		*/
		const handler = (value, error = void 0) => {
			if (dev_fallback_default) reactivity_loss_tracker = null;
			decrement_pending?.();
			deferreds.delete(d);
			if (error === OBSOLETE) return;
			batch.activate();
			if (error) {
				signal.f |= ERROR_VALUE;
				internal_set(signal, error);
			} else {
				if ((signal.f & 8388608) !== 0) signal.f ^= ERROR_VALUE;
				if (dev_fallback_default && location !== void 0 && !signal.equals(value)) {
					recent_async_deriveds.add(signal);
					setTimeout(() => {
						if (recent_async_deriveds.has(signal) && (effect.f & 16384) === 0) {
							await_waterfall(signal.label, location);
							recent_async_deriveds.delete(signal);
						}
					});
				}
				internal_set(signal, value);
			}
			batch.deactivate();
		};
		d.promise.then(handler, (e) => handler(null, e || "unknown"));
	});
	teardown(() => {
		for (const d of deferreds) d.reject(OBSOLETE);
	});
	if (dev_fallback_default) signal.f |= ASYNC;
	return new Promise((fulfil) => {
		/** @param {Promise<V>} p */
		function next(p) {
			function go() {
				if (p === promise) fulfil(signal);
				else next(promise);
			}
			p.then(go, go);
		}
		next(promise);
	});
}
/**
* @template V
* @param {() => V} fn
* @returns {Derived<V>}
*/
/*#__NO_SIDE_EFFECTS__*/
function user_derived(fn) {
	const d = /* @__PURE__ */ derived(fn);
	if (!async_mode_flag) push_reaction_value(d);
	return d;
}
/**
* @template V
* @param {() => V} fn
* @returns {Derived<V>}
*/
/*#__NO_SIDE_EFFECTS__*/
function derived_safe_equal(fn) {
	const signal = /* @__PURE__ */ derived(fn);
	signal.equals = safe_equals;
	return signal;
}
/**
* @param {Derived} derived
* @returns {void}
*/
function destroy_derived_effects(derived) {
	var effects = derived.effects;
	if (effects !== null) {
		derived.effects = null;
		for (var i = 0; i < effects.length; i += 1) destroy_effect(effects[i]);
	}
}
/**
* The currently updating deriveds, used to detect infinite recursion
* in dev mode and provide a nicer error than 'too much recursion'
* @type {Derived[]}
*/
var stack = [];
/**
* @template T
* @param {Derived} derived
* @returns {T}
*/
function execute_derived(derived) {
	var value;
	var prev_active_effect = active_effect;
	var parent = derived.parent;
	if (!is_destroying_effect && parent !== null && derived.v !== UNINITIALIZED && (parent.f & 24576) !== 0) {
		derived_inert();
		return derived.v;
	}
	set_active_effect(parent);
	if (dev_fallback_default) {
		let prev_eager_effects = eager_effects;
		set_eager_effects(/* @__PURE__ */ new Set());
		try {
			if (includes.call(stack, derived)) derived_references_self();
			stack.push(derived);
			derived.f &= ~WAS_MARKED;
			destroy_derived_effects(derived);
			value = update_reaction(derived);
		} finally {
			set_active_effect(prev_active_effect);
			set_eager_effects(prev_eager_effects);
			stack.pop();
		}
	} else try {
		derived.f &= ~WAS_MARKED;
		destroy_derived_effects(derived);
		value = update_reaction(derived);
	} finally {
		set_active_effect(prev_active_effect);
	}
	return value;
}
/**
* @param {Derived} derived
* @returns {void}
*/
function update_derived(derived) {
	var value = execute_derived(derived);
	if (!derived.equals(value)) {
		derived.wv = increment_write_version();
		if (!current_batch?.is_fork || derived.deps === null) {
			if (current_batch !== null) {
				current_batch.capture(derived, value, true);
				previous_batch?.capture(derived, value, true);
			} else derived.v = value;
			if (derived.deps === null) {
				set_signal_status(derived, CLEAN);
				return;
			}
		}
	}
	if (is_destroying_effect) return;
	if (batch_values !== null) {
		if (effect_tracking() || current_batch?.is_fork) batch_values.set(derived, value);
	} else update_derived_status(derived);
}
/**
* @param {Derived} derived
*/
function freeze_derived_effects(derived) {
	if (derived.effects === null) return;
	for (const e of derived.effects) if (e.teardown || e.ac) {
		e.teardown?.();
		e.ac?.abort(STALE_REACTION);
		if (e.fn !== null) e.teardown = noop;
		e.ac = null;
		remove_reactions(e, 0);
		destroy_effect_children(e);
	}
}
/**
* @param {Derived} derived
*/
function unfreeze_derived_effects(derived) {
	if (derived.effects === null) return;
	for (const e of derived.effects) if (e.teardown && e.fn !== null) update_effect(e);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/batch.js
/** @import { Fork } from 'svelte' */
/** @import { Derived, Effect, Reaction, Source, Value } from '#client' */
/** @type {Batch | null} */
var first_batch = null;
/** @type {Batch | null} */
var last_batch = null;
/** @type {Batch | null} */
var current_batch = null;
/**
* This is needed to avoid overwriting inputs
* @type {Batch | null}
*/
var previous_batch = null;
/**
* When time travelling (i.e. working in one batch, while other batches
* still have ongoing work), we ignore the real values of affected
* signals in favour of their values within the batch
* @type {Map<Value, any> | null}
*/
var batch_values = null;
/** @type {Effect | null} */
var last_scheduled_effect = null;
var is_flushing_sync = false;
var is_processing = false;
/**
* During traversal, this is an array. Newly created effects are (if not immediately
* executed) pushed to this array, rather than going through the scheduling
* rigamarole that would cause another turn of the flush loop.
* @type {Effect[] | null}
*/
var collected_effects = null;
/**
* An array of effects that are marked during traversal as a result of a `set`
* (not `internal_set`) call. These will be added to the next batch and
* trigger another `batch.process()`
* @type {Effect[] | null}
* @deprecated when we get rid of legacy mode and stores, we can get rid of this
*/
var legacy_updates = null;
var flush_count = 0;
/** @type {Set<Value>} */
var source_stacks = /* @__PURE__ */ new Set();
var uid = 1;
var Batch = class Batch {
	id = uid++;
	/** True as soon as `#process` was called */
	#started = false;
	linked = true;
	/** @type {Batch | null} */
	#prev = null;
	/** @type {Batch | null} */
	#next = null;
	/** @type {Map<Effect, ReturnType<typeof deferred<any>>>} */
	async_deriveds = /* @__PURE__ */ new Map();
	/**
	* The current values of any signals that are updated in this batch.
	* Tuple format: [value, is_derived] (note: is_derived is false for deriveds, too, if they were overridden via assignment)
	* They keys of this map are identical to `this.#previous`
	* @type {Map<Value, [any, boolean]>}
	*/
	current = /* @__PURE__ */ new Map();
	/**
	* The values of any signals (sources and deriveds) that are updated in this batch _before_ those updates took place.
	* They keys of this map are identical to `this.#current`
	* @type {Map<Value, any>}
	*/
	previous = /* @__PURE__ */ new Map();
	/**
	* When the batch is committed (and the DOM is updated), we need to remove old branches
	* and append new ones by calling the functions added inside (if/each/key/etc) blocks
	* @type {Set<(batch: Batch) => void>}
	*/
	#commit_callbacks = /* @__PURE__ */ new Set();
	/**
	* If a fork is discarded, we need to destroy any effects that are no longer needed
	* @type {Set<(batch: Batch) => void>}
	*/
	#discard_callbacks = /* @__PURE__ */ new Set();
	/**
	* The number of async effects that are currently in flight
	*/
	#pending = 0;
	/**
	* Async effects that are currently in flight, _not_ inside a pending boundary
	* @type {Map<Effect, number>}
	*/
	#blocking_pending = /* @__PURE__ */ new Map();
	/**
	* A deferred that resolves when the batch is committed, used with `settled()`
	* TODO replace with Promise.withResolvers once supported widely enough
	* @type {{ promise: Promise<void>, resolve: (value?: any) => void, reject: (reason: unknown) => void } | null}
	*/
	#deferred = null;
	/**
	* The root effects that need to be flushed
	* @type {Effect[]}
	*/
	#roots = [];
	/**
	* Effects created while this batch was active.
	* @type {Effect[]}
	*/
	#new_effects = [];
	/**
	* Deferred effects (which run after async work has completed) that are DIRTY
	* @type {Set<Effect>}
	*/
	#dirty_effects = /* @__PURE__ */ new Set();
	/**
	* Deferred effects that are MAYBE_DIRTY
	* @type {Set<Effect>}
	*/
	#maybe_dirty_effects = /* @__PURE__ */ new Set();
	/**
	* A map of branches that still exist, but will be destroyed when this batch
	* is committed — we skip over these during `process`.
	* The value contains child effects that were dirty/maybe_dirty before being reset,
	* so they can be rescheduled if the branch survives.
	* @type {Map<Effect, { d: Effect[], m: Effect[] }>}
	*/
	#skipped_branches = /* @__PURE__ */ new Map();
	/**
	* Inverse of #skipped_branches which we need to tell prior batches to unskip them when committing
	* @type {Set<Effect>}
	*/
	#unskipped_branches = /* @__PURE__ */ new Set();
	is_fork = false;
	#decrement_queued = false;
	constructor() {
		if (last_batch === null) first_batch = last_batch = this;
		else {
			last_batch.#next = this;
			this.#prev = last_batch;
		}
		last_batch = this;
	}
	#is_deferred() {
		if (this.is_fork) return true;
		for (const effect of this.#blocking_pending.keys()) {
			var e = effect;
			var skipped = false;
			while (e.parent !== null) {
				if (this.#skipped_branches.has(e)) {
					skipped = true;
					break;
				}
				e = e.parent;
			}
			if (!skipped) return true;
		}
		return false;
	}
	/**
	* Add an effect to the #skipped_branches map and reset its children
	* @param {Effect} effect
	*/
	skip_effect(effect) {
		if (!this.#skipped_branches.has(effect)) this.#skipped_branches.set(effect, {
			d: [],
			m: []
		});
		this.#unskipped_branches.delete(effect);
	}
	/**
	* Remove an effect from the #skipped_branches map and reschedule
	* any tracked dirty/maybe_dirty child effects
	* @param {Effect} effect
	* @param {(e: Effect) => void} callback
	*/
	unskip_effect(effect, callback = (e) => this.schedule(e)) {
		var tracked = this.#skipped_branches.get(effect);
		if (tracked) {
			this.#skipped_branches.delete(effect);
			for (var e of tracked.d) {
				set_signal_status(e, DIRTY);
				callback(e);
			}
			for (e of tracked.m) {
				set_signal_status(e, MAYBE_DIRTY);
				callback(e);
			}
		}
		this.#unskipped_branches.add(effect);
	}
	#process() {
		this.#started = true;
		if (flush_count++ > 1e3) {
			this.#unlink();
			infinite_loop_guard();
		}
		if (dev_fallback_default) for (const value of this.current.keys()) source_stacks.add(value);
		for (const e of this.#dirty_effects) {
			this.#maybe_dirty_effects.delete(e);
			set_signal_status(e, DIRTY);
			this.schedule(e);
		}
		for (const e of this.#maybe_dirty_effects) {
			set_signal_status(e, MAYBE_DIRTY);
			this.schedule(e);
		}
		const roots = this.#roots;
		this.#roots = [];
		this.apply();
		/** @type {Effect[]} */
		var effects = collected_effects = [];
		/** @type {Effect[]} */
		var render_effects = [];
		/**
		* @type {Effect[]}
		* @deprecated when we get rid of legacy mode and stores, we can get rid of this
		*/
		var updates = legacy_updates = [];
		for (const root of roots) try {
			this.#traverse(root, effects, render_effects);
		} catch (e) {
			reset_all(root);
			if (!this.#is_deferred()) this.discard();
			throw e;
		}
		current_batch = null;
		if (updates.length > 0) {
			var batch = Batch.ensure();
			for (const e of updates) batch.schedule(e);
		}
		collected_effects = null;
		legacy_updates = null;
		if (this.#is_deferred()) {
			this.#defer_effects(render_effects);
			this.#defer_effects(effects);
			for (const [e, t] of this.#skipped_branches) reset_branch(e, t);
			if (updates.length > 0)
 /** @type {Batch} */ current_batch.#process();
			return;
		}
		const earlier_batch = this.#find_earlier_batch();
		if (earlier_batch) {
			this.#defer_effects(render_effects);
			this.#defer_effects(effects);
			earlier_batch.#merge(this);
			return;
		}
		this.#dirty_effects.clear();
		this.#maybe_dirty_effects.clear();
		for (const fn of this.#commit_callbacks) fn(this);
		this.#commit_callbacks.clear();
		previous_batch = this;
		flush_queued_effects(render_effects);
		flush_queued_effects(effects);
		previous_batch = null;
		this.#deferred?.resolve();
		var next_batch = current_batch;
		if (this.#pending === 0 && (this.#roots.length === 0 || next_batch !== null)) {
			this.#unlink();
			if (async_mode_flag) {
				this.#commit();
				current_batch = next_batch;
			}
		}
		if (this.#roots.length > 0) if (next_batch !== null) {
			const batch = next_batch;
			batch.#roots.push(...this.#roots.filter((r) => !batch.#roots.includes(r)));
		} else next_batch = this;
		if (next_batch !== null) next_batch.#process();
	}
	/**
	* Traverse the effect tree, executing effects or stashing
	* them for later execution as appropriate
	* @param {Effect} root
	* @param {Effect[]} effects
	* @param {Effect[]} render_effects
	*/
	#traverse(root, effects, render_effects) {
		root.f ^= CLEAN;
		var effect = root.first;
		while (effect !== null) {
			var flags = effect.f;
			var is_branch = (flags & 96) !== 0;
			if (!(is_branch && (flags & 1024) !== 0 || (flags & 8192) !== 0 || this.#skipped_branches.has(effect)) && effect.fn !== null) {
				if (is_branch) effect.f ^= CLEAN;
				else if ((flags & 4) !== 0) effects.push(effect);
				else if (async_mode_flag && (flags & 16777224) !== 0) render_effects.push(effect);
				else if (is_dirty(effect)) {
					if ((flags & 16) !== 0) this.#maybe_dirty_effects.add(effect);
					update_effect(effect);
				}
				var child = effect.first;
				if (child !== null) {
					effect = child;
					continue;
				}
			}
			while (effect !== null) {
				var next = effect.next;
				if (next !== null) {
					effect = next;
					break;
				}
				effect = effect.parent;
			}
		}
	}
	#find_earlier_batch() {
		var batch = this.#prev;
		while (batch !== null) {
			if (!batch.is_fork) {
				for (const [value, [, is_derived]] of this.current) if (batch.current.has(value) && !is_derived) return batch;
			}
			batch = batch.#prev;
		}
		return null;
	}
	/**
	* @param {Batch} batch
	*/
	#merge(batch) {
		for (const [source, value] of batch.current) {
			if (!this.previous.has(source) && batch.previous.has(source)) this.previous.set(source, batch.previous.get(source));
			this.current.set(source, value);
		}
		for (const [effect, deferred] of batch.async_deriveds) {
			const d = this.async_deriveds.get(effect);
			if (d) deferred.promise.then(d.resolve).catch(d.reject);
		}
		batch.async_deriveds.clear();
		this.transfer_effects(batch.#dirty_effects, batch.#maybe_dirty_effects);
		/**
		* mark all effects that depend on `batch.current`, except the
		* async effects that we just resolved (TODO unless they depend
		* on values in this batch that are NOT in the later batch?).
		* Through this we also will populate the correct #skipped_branches,
		* oncommit callbacks etc, so we don't need to merge them separately.
		* @param {Value} value
		*/
		const mark = (value) => {
			var reactions = value.reactions;
			if (reactions === null) return;
			for (const reaction of reactions) {
				var flags = reaction.f;
				if ((flags & 2) !== 0) mark(reaction);
				else {
					var effect = reaction;
					if (flags & 4194320 && !this.async_deriveds.has(effect)) {
						this.#maybe_dirty_effects.delete(effect);
						set_signal_status(effect, DIRTY);
						this.schedule(effect);
					}
				}
			}
		};
		for (const source of this.current.keys()) mark(source);
		this.oncommit(() => batch.discard());
		batch.#unlink();
		current_batch = this;
		this.#process();
	}
	/**
	* @param {Effect[]} effects
	*/
	#defer_effects(effects) {
		for (var i = 0; i < effects.length; i += 1) defer_effect(effects[i], this.#dirty_effects, this.#maybe_dirty_effects);
	}
	/**
	* Associate a change to a given source with the current
	* batch, noting its previous and current values
	* @param {Value} source
	* @param {any} value
	* @param {boolean} [is_derived]
	*/
	capture(source, value, is_derived = false) {
		if (source.v !== UNINITIALIZED && !this.previous.has(source)) this.previous.set(source, source.v);
		if ((source.f & 8388608) === 0) {
			this.current.set(source, [value, is_derived]);
			batch_values?.set(source, value);
		}
		if (!this.is_fork) source.v = value;
	}
	activate() {
		current_batch = this;
	}
	deactivate() {
		current_batch = null;
		batch_values = null;
	}
	flush() {
		try {
			if (dev_fallback_default) source_stacks.clear();
			is_processing = true;
			current_batch = this;
			this.#process();
		} finally {
			flush_count = 0;
			last_scheduled_effect = null;
			collected_effects = null;
			legacy_updates = null;
			is_processing = false;
			current_batch = null;
			batch_values = null;
			old_values.clear();
			if (dev_fallback_default) for (const source of source_stacks) source.updated = null;
		}
	}
	discard() {
		for (const fn of this.#discard_callbacks) fn(this);
		this.#discard_callbacks.clear();
		for (const deferred of this.async_deriveds.values()) deferred.reject(OBSOLETE);
		this.#unlink();
		this.#deferred?.resolve();
	}
	/**
	* @param {Effect} effect
	*/
	register_created_effect(effect) {
		this.#new_effects.push(effect);
	}
	#commit() {
		for (let batch = first_batch; batch !== null; batch = batch.#next) {
			var is_earlier = batch.id < this.id;
			/** @type {Source[]} */
			var sources = [];
			for (const [source, [value, is_derived]] of this.current) {
				if (batch.current.has(source)) {
					var batch_value = batch.current.get(source)[0];
					if (is_earlier && value !== batch_value) batch.current.set(source, [value, is_derived]);
					else continue;
				}
				sources.push(source);
			}
			if (is_earlier) for (const [effect, deferred] of this.async_deriveds) {
				const d = batch.async_deriveds.get(effect);
				if (d) deferred.promise.then(d.resolve).catch(d.reject);
			}
			var current = [...batch.current.keys()].filter((source) => !batch.current.get(source)[1]);
			if (!batch.#started || current.length === 0) continue;
			var others = current.filter((source) => !this.current.has(source));
			if (others.length === 0) {
				if (is_earlier) batch.discard();
			} else if (sources.length > 0) {
				if (dev_fallback_default && !batch.#decrement_queued) invariant(batch.#roots.length === 0, "Batch has scheduled roots");
				if (is_earlier) for (const unskipped of this.#unskipped_branches) batch.unskip_effect(unskipped, (e) => {
					if ((e.f & 4194320) !== 0) batch.schedule(e);
					else batch.#defer_effects([e]);
				});
				batch.activate();
				/** @type {Set<Value>} */
				var marked = /* @__PURE__ */ new Set();
				/** @type {Map<Reaction, boolean>} */
				var checked = /* @__PURE__ */ new Map();
				for (var source of sources) mark_effects(source, others, marked, checked);
				checked = /* @__PURE__ */ new Map();
				var current_unequal = [...batch.current].filter(([c, v1]) => {
					const v2 = this.current.get(c);
					if (!v2) return true;
					return v2[0] !== v1[0] || v2[1] !== v1[1];
				}).map(([c]) => c);
				if (current_unequal.length > 0) {
					for (const effect of this.#new_effects) if ((effect.f & 155648) === 0 && depends_on(effect, current_unequal, checked)) if ((effect.f & 4194320) !== 0) {
						set_signal_status(effect, DIRTY);
						batch.schedule(effect);
					} else batch.#dirty_effects.add(effect);
				}
				if (batch.#roots.length > 0 && !batch.#decrement_queued) {
					batch.apply();
					for (var root of batch.#roots) batch.#traverse(root, [], []);
					batch.#roots = [];
				}
				batch.deactivate();
			}
		}
	}
	/**
	* @param {boolean} blocking
	* @param {Effect} effect
	*/
	increment(blocking, effect) {
		this.#pending += 1;
		if (blocking) {
			let blocking_pending_count = this.#blocking_pending.get(effect) ?? 0;
			this.#blocking_pending.set(effect, blocking_pending_count + 1);
		}
	}
	/**
	* @param {boolean} blocking
	* @param {Effect} effect
	*/
	decrement(blocking, effect) {
		this.#pending -= 1;
		if (blocking) {
			let blocking_pending_count = this.#blocking_pending.get(effect) ?? 0;
			if (blocking_pending_count === 1) this.#blocking_pending.delete(effect);
			else this.#blocking_pending.set(effect, blocking_pending_count - 1);
		}
		if (this.#decrement_queued) return;
		this.#decrement_queued = true;
		queue_micro_task(() => {
			this.#decrement_queued = false;
			if (this.linked) this.flush();
		});
	}
	/**
	* @param {Set<Effect>} dirty_effects
	* @param {Set<Effect>} maybe_dirty_effects
	*/
	transfer_effects(dirty_effects, maybe_dirty_effects) {
		for (const e of dirty_effects) this.#dirty_effects.add(e);
		for (const e of maybe_dirty_effects) this.#maybe_dirty_effects.add(e);
		dirty_effects.clear();
		maybe_dirty_effects.clear();
	}
	/** @param {(batch: Batch) => void} fn */
	oncommit(fn) {
		this.#commit_callbacks.add(fn);
	}
	/** @param {(batch: Batch) => void} fn */
	ondiscard(fn) {
		this.#discard_callbacks.add(fn);
	}
	settled() {
		return (this.#deferred ??= deferred()).promise;
	}
	static ensure() {
		if (current_batch === null) {
			const batch = current_batch = new Batch();
			if (!is_processing && !is_flushing_sync) queue_micro_task(() => {
				if (!batch.#started) batch.flush();
			});
		}
		return current_batch;
	}
	apply() {
		if (!async_mode_flag || !this.is_fork && this.#prev === null && this.#next === null) {
			batch_values = null;
			return;
		}
		batch_values = /* @__PURE__ */ new Map();
		for (const [source, [value]] of this.current) batch_values.set(source, value);
		for (let batch = first_batch; batch !== null; batch = batch.#next) {
			if (batch === this || batch.is_fork) continue;
			var intersects = false;
			if (batch.id < this.id) for (const [source, [, is_derived]] of batch.current) {
				if (is_derived) continue;
				if (this.current.has(source)) {
					intersects = true;
					break;
				}
			}
			if (!intersects) {
				for (const [source, previous] of batch.previous) if (!batch_values.has(source)) batch_values.set(source, previous);
			}
		}
	}
	/**
	*
	* @param {Effect} effect
	*/
	schedule(effect) {
		last_scheduled_effect = effect;
		if (effect.b?.is_pending && (effect.f & 16777228) !== 0 && (effect.f & 32768) === 0) {
			effect.b.defer_effect(effect);
			return;
		}
		var e = effect;
		while (e.parent !== null) {
			e = e.parent;
			var flags = e.f;
			if (collected_effects !== null && e === active_effect) {
				if (async_mode_flag) return;
				if ((active_reaction === null || (active_reaction.f & 2) === 0) && !legacy_is_updating_store) return;
			}
			if ((flags & 96) !== 0) {
				if ((flags & 1024) === 0) return;
				e.f ^= CLEAN;
			}
		}
		this.#roots.push(e);
	}
	#unlink() {
		if (!this.linked) return;
		var prev = this.#prev;
		var next = this.#next;
		if (prev === null) first_batch = next;
		else prev.#next = next;
		if (next === null) last_batch = prev;
		else next.#prev = prev;
		this.linked = false;
	}
};
/**
* Synchronously flush any pending updates.
* Returns void if no callback is provided, otherwise returns the result of calling the callback.
* @template [T=void]
* @param {(() => T) | undefined} [fn]
* @returns {T}
*/
function flushSync(fn) {
	var was_flushing_sync = is_flushing_sync;
	is_flushing_sync = true;
	try {
		var result;
		if (fn) {
			if (current_batch !== null && !current_batch.is_fork) current_batch.flush();
			result = fn();
		}
		while (true) {
			flush_tasks();
			if (current_batch === null) return result;
			current_batch.flush();
		}
	} finally {
		is_flushing_sync = was_flushing_sync;
	}
}
function infinite_loop_guard() {
	if (dev_fallback_default) {
		var updates = /* @__PURE__ */ new Map();
		for (const source of current_batch.current.keys()) for (const [stack, update] of source.updated ?? []) {
			var entry = updates.get(stack);
			if (!entry) {
				entry = {
					error: update.error,
					count: 0
				};
				updates.set(stack, entry);
			}
			entry.count += update.count;
		}
		for (const update of updates.values()) if (update.error) console.error(update.error);
	}
	try {
		effect_update_depth_exceeded();
	} catch (error) {
		if (dev_fallback_default) define_property(error, "stack", { value: "" });
		invoke_error_boundary(error, last_scheduled_effect);
	}
}
/** @type {Set<Effect> | null} */
var eager_block_effects = null;
/**
* @param {Array<Effect>} effects
* @returns {void}
*/
function flush_queued_effects(effects) {
	var length = effects.length;
	if (length === 0) return;
	var i = 0;
	while (i < length) {
		var effect = effects[i++];
		if ((effect.f & 24576) === 0 && is_dirty(effect)) {
			eager_block_effects = /* @__PURE__ */ new Set();
			update_effect(effect);
			if (effect.deps === null && effect.first === null && effect.nodes === null && effect.teardown === null && effect.ac === null) unlink_effect(effect);
			if (eager_block_effects?.size > 0) {
				old_values.clear();
				for (const e of eager_block_effects) {
					if ((e.f & 24576) !== 0) continue;
					/** @type {Effect[]} */
					const ordered_effects = [e];
					let ancestor = e.parent;
					while (ancestor !== null) {
						if (eager_block_effects.has(ancestor)) {
							eager_block_effects.delete(ancestor);
							ordered_effects.push(ancestor);
						}
						ancestor = ancestor.parent;
					}
					for (let j = ordered_effects.length - 1; j >= 0; j--) {
						const e = ordered_effects[j];
						if ((e.f & 24576) !== 0) continue;
						update_effect(e);
					}
				}
				eager_block_effects.clear();
			}
		}
	}
	eager_block_effects = null;
}
/**
* This is similar to `mark_reactions`, but it only marks async/block effects
* depending on `value` and at least one of the other `sources`, so that
* these effects can re-run after another batch has been committed
* @param {Value} value
* @param {Source[]} sources
* @param {Set<Value>} marked
* @param {Map<Reaction, boolean>} checked
*/
function mark_effects(value, sources, marked, checked) {
	if (marked.has(value)) return;
	marked.add(value);
	if (value.reactions !== null) for (const reaction of value.reactions) {
		const flags = reaction.f;
		if ((flags & 2) !== 0) mark_effects(reaction, sources, marked, checked);
		else if ((flags & 4194320) !== 0 && (flags & 2048) === 0 && depends_on(reaction, sources, checked)) {
			set_signal_status(reaction, DIRTY);
			schedule_effect(reaction);
		}
	}
}
/**
* @param {Reaction} reaction
* @param {Source[]} sources
* @param {Map<Reaction, boolean>} checked
*/
function depends_on(reaction, sources, checked) {
	const depends = checked.get(reaction);
	if (depends !== void 0) return depends;
	if (reaction.deps !== null) for (const dep of reaction.deps) {
		if (includes.call(sources, dep)) return true;
		if ((dep.f & 2) !== 0 && depends_on(dep, sources, checked)) {
			checked.set(dep, true);
			return true;
		}
	}
	checked.set(reaction, false);
	return false;
}
/**
* @param {Effect} effect
* @returns {void}
*/
function schedule_effect(effect) {
	/** @type {Batch} */ current_batch.schedule(effect);
}
/**
* Mark all the effects inside a skipped branch CLEAN, so that
* they can be correctly rescheduled later. Tracks dirty and maybe_dirty
* effects so they can be rescheduled if the branch survives.
* @param {Effect} effect
* @param {{ d: Effect[], m: Effect[] }} tracked
*/
function reset_branch(effect, tracked) {
	if ((effect.f & 32) !== 0 && (effect.f & 1024) !== 0) return;
	if ((effect.f & 2048) !== 0) tracked.d.push(effect);
	else if ((effect.f & 4096) !== 0) tracked.m.push(effect);
	set_signal_status(effect, CLEAN);
	var e = effect.first;
	while (e !== null) {
		reset_branch(e, tracked);
		e = e.next;
	}
}
/**
* Mark an entire effect tree clean following an error
* @param {Effect} effect
*/
function reset_all(effect) {
	set_signal_status(effect, CLEAN);
	var e = effect.first;
	while (e !== null) {
		reset_all(e);
		e = e.next;
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/sources.js
/** @import { Derived, Effect, Source, Value } from '#client' */
/** @type {Set<Effect>} */
var eager_effects = /* @__PURE__ */ new Set();
/** @type {Map<Source, any>} */
var old_values = /* @__PURE__ */ new Map();
/**
* @param {Set<any>} v
*/
function set_eager_effects(v) {
	eager_effects = v;
}
var eager_effects_deferred = false;
function set_eager_effects_deferred() {
	eager_effects_deferred = true;
}
/**
* @template V
* @param {V} v
* @param {Error | null} [stack]
* @returns {Source<V>}
*/
function source(v, stack) {
	/** @type {Value} */
	var signal = {
		f: 0,
		v,
		reactions: null,
		equals,
		rv: 0,
		wv: 0
	};
	if (dev_fallback_default && tracing_mode_flag) {
		signal.created = stack ?? get_error("created at");
		signal.updated = null;
		signal.set_during_effect = false;
		signal.trace = null;
	}
	return signal;
}
/**
* @template V
* @param {V} v
* @param {Error | null} [stack]
*/
/*#__NO_SIDE_EFFECTS__*/
function state(v, stack) {
	const s = source(v, stack);
	push_reaction_value(s);
	return s;
}
/**
* @template V
* @param {V} initial_value
* @param {boolean} [immutable]
* @returns {Source<V>}
*/
/*#__NO_SIDE_EFFECTS__*/
function mutable_source(initial_value, immutable = false, trackable = true) {
	const s = source(initial_value);
	if (!immutable) s.equals = safe_equals;
	if (legacy_mode_flag && trackable && component_context !== null && component_context.l !== null) (component_context.l.s ??= []).push(s);
	return s;
}
/**
* @template V
* @param {Source<V>} source
* @param {V} value
* @param {boolean} [should_proxy]
* @returns {V}
*/
function set(source, value, should_proxy = false) {
	if (active_reaction !== null && (!untracking || (active_reaction.f & 131072) !== 0) && is_runes() && (active_reaction.f & 4325394) !== 0 && (current_sources === null || !current_sources.has(source))) state_unsafe_mutation();
	let new_value = should_proxy ? proxy(value) : value;
	if (dev_fallback_default) tag_proxy(new_value, source.label);
	return internal_set(source, new_value, legacy_updates);
}
/**
* @template V
* @param {Source<V>} source
* @param {V} value
* @param {Effect[] | null} [updated_during_traversal]
* @returns {V}
*/
function internal_set(source, value, updated_during_traversal = null) {
	if (!source.equals(value)) {
		old_values.set(source, is_destroying_effect ? value : source.v);
		var batch = Batch.ensure();
		batch.capture(source, value);
		if (dev_fallback_default) {
			if (tracing_mode_flag || active_effect !== null) {
				source.updated ??= /* @__PURE__ */ new Map();
				const count = (source.updated.get("")?.count ?? 0) + 1;
				source.updated.set("", {
					error: null,
					count
				});
				if (tracing_mode_flag || count > 5) {
					const error = get_error("updated at");
					if (error !== null) {
						let entry = source.updated.get(error.stack);
						if (!entry) {
							entry = {
								error,
								count: 0
							};
							source.updated.set(error.stack, entry);
						}
						entry.count++;
					}
				}
			}
			if (active_effect !== null) source.set_during_effect = true;
		}
		if ((source.f & 2) !== 0) {
			const derived = source;
			if ((source.f & 2048) !== 0) execute_derived(derived);
			if (batch_values === null) update_derived_status(derived);
		}
		source.wv = increment_write_version();
		mark_reactions(source, DIRTY, updated_during_traversal);
		if (is_runes() && active_effect !== null && (active_effect.f & 1024) !== 0 && (active_effect.f & 96) === 0) if (untracked_writes === null) set_untracked_writes([source]);
		else untracked_writes.push(source);
		if (!batch.is_fork && eager_effects.size > 0 && !eager_effects_deferred) flush_eager_effects();
	}
	return value;
}
function flush_eager_effects() {
	eager_effects_deferred = false;
	for (const effect of eager_effects) {
		if ((effect.f & 1024) !== 0) set_signal_status(effect, MAYBE_DIRTY);
		let dirty;
		try {
			dirty = is_dirty(effect);
		} catch {
			dirty = true;
		}
		if (dirty) update_effect(effect);
	}
	eager_effects.clear();
}
/**
* Silently (without using `get`) increment a source
* @param {Source<number>} source
*/
function increment(source) {
	set(source, source.v + 1);
}
/**
* @param {Value} signal
* @param {number} status should be DIRTY or MAYBE_DIRTY
* @param {Effect[] | null} updated_during_traversal
* @returns {void}
*/
function mark_reactions(signal, status, updated_during_traversal) {
	var reactions = signal.reactions;
	if (reactions === null) return;
	var runes = is_runes();
	var length = reactions.length;
	for (var i = 0; i < length; i++) {
		var reaction = reactions[i];
		var flags = reaction.f;
		if (!runes && reaction === active_effect) continue;
		var not_dirty = (flags & DIRTY) === 0;
		if (not_dirty) set_signal_status(reaction, status);
		if ((flags & 131072) !== 0) eager_effects.add(reaction);
		else if ((flags & 2) !== 0) {
			var derived = reaction;
			batch_values?.delete(derived);
			if ((flags & 65536) === 0) {
				if (flags & 512 && (active_effect === null || (active_effect.f & 2097152) === 0)) reaction.f |= WAS_MARKED;
				mark_reactions(derived, MAYBE_DIRTY, updated_during_traversal);
			}
		} else if (not_dirty) {
			var effect = reaction;
			if ((flags & 16) !== 0 && eager_block_effects !== null) eager_block_effects.add(effect);
			if (updated_during_traversal !== null) updated_during_traversal.push(effect);
			else schedule_effect(effect);
		}
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/proxy.js
/** @import { Source } from '#client' */
var regex_is_valid_identifier = /^[a-zA-Z_$][a-zA-Z_$0-9]*$/;
/**
* @template T
* @param {T} value
* @returns {T}
*/
function proxy(value) {
	if (typeof value !== "object" || value === null || STATE_SYMBOL in value) return value;
	const prototype = get_prototype_of(value);
	if (prototype !== object_prototype && prototype !== array_prototype) return value;
	/** @type {Map<any, Source<any>>} */
	var sources = /* @__PURE__ */ new Map();
	var is_proxied_array = is_array(value);
	var version = /* @__PURE__ */ state(0);
	var stack = dev_fallback_default && tracing_mode_flag ? get_error("created at") : null;
	var parent_version = update_version;
	/**
	* Executes the proxy in the context of the reaction it was originally created in, if any
	* @template T
	* @param {() => T} fn
	*/
	var with_parent = (fn) => {
		if (update_version === parent_version) return fn();
		var reaction = active_reaction;
		var version = update_version;
		set_active_reaction(null);
		set_update_version(parent_version);
		var result = fn();
		set_active_reaction(reaction);
		set_update_version(version);
		return result;
	};
	if (is_proxied_array) {
		sources.set("length", /* @__PURE__ */ state(
			/** @type {any[]} */
			value.length,
			stack
		));
		if (dev_fallback_default) value = inspectable_array(value);
	}
	/** Used in dev for $inspect.trace() */
	var path = "";
	let updating = false;
	/** @param {string} new_path */
	function update_path(new_path) {
		if (updating) return;
		updating = true;
		path = new_path;
		tag(version, `${path} version`);
		for (const [prop, source] of sources) tag(source, get_label(path, prop));
		updating = false;
	}
	return new Proxy(value, {
		defineProperty(_, prop, descriptor) {
			if (!("value" in descriptor) || descriptor.configurable === false || descriptor.enumerable === false || descriptor.writable === false) state_descriptors_fixed();
			var s = sources.get(prop);
			if (s === void 0) with_parent(() => {
				var s = /* @__PURE__ */ state(descriptor.value, stack);
				sources.set(prop, s);
				if (dev_fallback_default && typeof prop === "string") tag(s, get_label(path, prop));
				return s;
			});
			else set(s, descriptor.value, true);
			return true;
		},
		deleteProperty(target, prop) {
			var s = sources.get(prop);
			if (s === void 0) {
				if (prop in target) {
					const s = with_parent(() => /* @__PURE__ */ state(UNINITIALIZED, stack));
					sources.set(prop, s);
					increment(version);
					if (dev_fallback_default) tag(s, get_label(path, prop));
				}
			} else {
				set(s, UNINITIALIZED);
				increment(version);
			}
			return true;
		},
		get(target, prop, receiver) {
			if (prop === STATE_SYMBOL) return value;
			if (dev_fallback_default && prop === PROXY_PATH_SYMBOL) return update_path;
			var s = sources.get(prop);
			var exists = prop in target;
			if (s === void 0 && (!exists || get_descriptor(target, prop)?.writable)) {
				s = with_parent(() => {
					var s = /* @__PURE__ */ state(proxy(exists ? target[prop] : UNINITIALIZED), stack);
					if (dev_fallback_default) tag(s, get_label(path, prop));
					return s;
				});
				sources.set(prop, s);
			}
			if (s !== void 0) {
				var v = get(s);
				return v === UNINITIALIZED ? void 0 : v;
			}
			return Reflect.get(target, prop, receiver);
		},
		getOwnPropertyDescriptor(target, prop) {
			var descriptor = Reflect.getOwnPropertyDescriptor(target, prop);
			if (descriptor && "value" in descriptor) {
				var s = sources.get(prop);
				if (s) descriptor.value = get(s);
			} else if (descriptor === void 0) {
				var source = sources.get(prop);
				var value = source?.v;
				if (source !== void 0 && value !== UNINITIALIZED) return {
					enumerable: true,
					configurable: true,
					value,
					writable: true
				};
			}
			return descriptor;
		},
		has(target, prop) {
			if (prop === STATE_SYMBOL) return true;
			var s = sources.get(prop);
			var has = s !== void 0 && s.v !== UNINITIALIZED || Reflect.has(target, prop);
			if (s !== void 0 || active_effect !== null && (!has || get_descriptor(target, prop)?.writable)) {
				if (s === void 0) {
					s = with_parent(() => {
						var s = /* @__PURE__ */ state(has ? proxy(target[prop]) : UNINITIALIZED, stack);
						if (dev_fallback_default) tag(s, get_label(path, prop));
						return s;
					});
					sources.set(prop, s);
				}
				if (get(s) === UNINITIALIZED) return false;
			}
			return has;
		},
		set(target, prop, value, receiver) {
			var s = sources.get(prop);
			var has = prop in target;
			if (is_proxied_array && prop === "length") for (var i = value; i < s.v; i += 1) {
				var other_s = sources.get(i + "");
				if (other_s !== void 0) set(other_s, UNINITIALIZED);
				else if (i in target) {
					other_s = with_parent(() => /* @__PURE__ */ state(UNINITIALIZED, stack));
					sources.set(i + "", other_s);
					if (dev_fallback_default) tag(other_s, get_label(path, i));
				}
			}
			if (s === void 0) {
				if (!has || get_descriptor(target, prop)?.writable) {
					s = with_parent(() => /* @__PURE__ */ state(void 0, stack));
					if (dev_fallback_default) tag(s, get_label(path, prop));
					set(s, proxy(value));
					sources.set(prop, s);
				}
			} else {
				has = s.v !== UNINITIALIZED;
				var p = with_parent(() => proxy(value));
				set(s, p);
			}
			var descriptor = Reflect.getOwnPropertyDescriptor(target, prop);
			if (descriptor?.set) descriptor.set.call(receiver, value);
			if (!has) {
				if (is_proxied_array && typeof prop === "string") {
					var ls = sources.get("length");
					var n = Number(prop);
					if (Number.isInteger(n) && n >= ls.v) set(ls, n + 1);
				}
				increment(version);
			}
			return true;
		},
		ownKeys(target) {
			get(version);
			var own_keys = Reflect.ownKeys(target).filter((key) => {
				var source = sources.get(key);
				return source === void 0 || source.v !== UNINITIALIZED;
			});
			for (var [key, source] of sources) if (source.v !== UNINITIALIZED && !(key in target)) own_keys.push(key);
			return own_keys;
		},
		setPrototypeOf() {
			state_prototype_fixed();
		}
	});
}
/**
* @param {string} path
* @param {string | symbol} prop
*/
function get_label(path, prop) {
	if (typeof prop === "symbol") return `${path}[Symbol(${prop.description ?? ""})]`;
	if (regex_is_valid_identifier.test(prop)) return `${path}.${prop}`;
	return /^\d+$/.test(prop) ? `${path}[${prop}]` : `${path}['${prop}']`;
}
/**
* @param {any} value
*/
function get_proxied_value(value) {
	try {
		if (value !== null && typeof value === "object" && STATE_SYMBOL in value) return value[STATE_SYMBOL];
	} catch {}
	return value;
}
/**
* @param {any} a
* @param {any} b
*/
function is(a, b) {
	return Object.is(get_proxied_value(a), get_proxied_value(b));
}
var ARRAY_MUTATING_METHODS = new Set([
	"copyWithin",
	"fill",
	"pop",
	"push",
	"reverse",
	"shift",
	"sort",
	"splice",
	"unshift"
]);
/**
* Wrap array mutating methods so $inspect is triggered only once and
* to prevent logging an array in intermediate state (e.g. with an empty slot)
* @param {any[]} array
*/
function inspectable_array(array) {
	return new Proxy(array, { get(target, prop, receiver) {
		var value = Reflect.get(target, prop, receiver);
		if (!ARRAY_MUTATING_METHODS.has(prop)) return value;
		/**
		* @this {any[]}
		* @param {any[]} args
		*/
		return function(...args) {
			set_eager_effects_deferred();
			var result = value.apply(this, args);
			flush_eager_effects();
			return result;
		};
	} });
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dev/equality.js
function init_array_prototype_warnings() {
	const array_prototype = Array.prototype;
	const cleanup = Array.__svelte_cleanup;
	if (cleanup) cleanup();
	const { indexOf, lastIndexOf, includes } = array_prototype;
	array_prototype.indexOf = function(item, from_index) {
		const index = indexOf.call(this, item, from_index);
		if (index === -1) {
			for (let i = from_index ?? 0; i < this.length; i += 1) if (get_proxied_value(this[i]) === item) {
				state_proxy_equality_mismatch("array.indexOf(...)");
				break;
			}
		}
		return index;
	};
	array_prototype.lastIndexOf = function(item, from_index) {
		const index = lastIndexOf.call(this, item, from_index ?? this.length - 1);
		if (index === -1) {
			for (let i = 0; i <= (from_index ?? this.length - 1); i += 1) if (get_proxied_value(this[i]) === item) {
				state_proxy_equality_mismatch("array.lastIndexOf(...)");
				break;
			}
		}
		return index;
	};
	array_prototype.includes = function(item, from_index) {
		const has = includes.call(this, item, from_index);
		if (!has) {
			for (let i = 0; i < this.length; i += 1) if (get_proxied_value(this[i]) === item) {
				state_proxy_equality_mismatch("array.includes(...)");
				break;
			}
		}
		return has;
	};
	Array.__svelte_cleanup = () => {
		array_prototype.indexOf = indexOf;
		array_prototype.lastIndexOf = lastIndexOf;
		array_prototype.includes = includes;
	};
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/operations.js
/** @import { Effect, TemplateNode } from '#client' */
/** @type {Window} */
var $window;
/** @type {boolean} */
var is_firefox;
/** @type {() => Node | null} */
var first_child_getter;
/** @type {() => Node | null} */
var next_sibling_getter;
/**
* Initialize these lazily to avoid issues when using the runtime in a server context
* where these globals are not available while avoiding a separate server entry point
*/
function init_operations() {
	if ($window !== void 0) return;
	$window = window;
	is_firefox = /Firefox/.test(navigator.userAgent);
	var element_prototype = Element.prototype;
	var node_prototype = Node.prototype;
	var text_prototype = Text.prototype;
	first_child_getter = get_descriptor(node_prototype, "firstChild").get;
	next_sibling_getter = get_descriptor(node_prototype, "nextSibling").get;
	if (is_extensible(element_prototype)) {
		/** @type {any} */ element_prototype[CLASS_CACHE] = void 0;
		/** @type {any} */ element_prototype[ATTRIBUTES_CACHE] = null;
		/** @type {any} */ element_prototype[STYLE_CACHE] = void 0;
		element_prototype.__e = void 0;
	}
	if (is_extensible(text_prototype))
 /** @type {any} */ text_prototype[TEXT_CACHE] = void 0;
	if (dev_fallback_default) {
		element_prototype.__svelte_meta = null;
		init_array_prototype_warnings();
	}
}
/**
* @param {string} value
* @returns {Text}
*/
function create_text(value = "") {
	return document.createTextNode(value);
}
/**
* @template {Node} N
* @param {N} node
*/
/*@__NO_SIDE_EFFECTS__*/
function get_first_child(node) {
	return first_child_getter.call(node);
}
/**
* @template {Node} N
* @param {N} node
*/
/*@__NO_SIDE_EFFECTS__*/
function get_next_sibling(node) {
	return next_sibling_getter.call(node);
}
/**
* Don't mark this as side-effect-free, hydration needs to walk all nodes
* @template {Node} N
* @param {N} node
* @param {boolean} is_text
* @returns {TemplateNode | null}
*/
function child(node, is_text) {
	if (!hydrating) return /* @__PURE__ */ get_first_child(node);
	var child = /* @__PURE__ */ get_first_child(hydrate_node);
	if (child === null) child = hydrate_node.appendChild(create_text());
	else if (is_text && child.nodeType !== 3) {
		var text = create_text();
		child?.before(text);
		set_hydrate_node(text);
		return text;
	}
	if (is_text) merge_text_nodes(child);
	set_hydrate_node(child);
	return child;
}
/**
* Don't mark this as side-effect-free, hydration needs to walk all nodes
* @param {TemplateNode} node
* @param {boolean} [is_text]
* @returns {TemplateNode | null}
*/
function first_child(node, is_text = false) {
	if (!hydrating) {
		var first = /* @__PURE__ */ get_first_child(node);
		if (first instanceof Comment && first.data === "") return /* @__PURE__ */ get_next_sibling(first);
		return first;
	}
	if (is_text) {
		if (hydrate_node?.nodeType !== 3) {
			var text = create_text();
			hydrate_node?.before(text);
			set_hydrate_node(text);
			return text;
		}
		merge_text_nodes(hydrate_node);
	}
	return hydrate_node;
}
/**
* Don't mark this as side-effect-free, hydration needs to walk all nodes
* @param {TemplateNode} node
* @param {number} count
* @param {boolean} is_text
* @returns {TemplateNode | null}
*/
function sibling(node, count = 1, is_text = false) {
	let next_sibling = hydrating ? hydrate_node : node;
	var last_sibling;
	while (count--) {
		last_sibling = next_sibling;
		next_sibling = /* @__PURE__ */ get_next_sibling(next_sibling);
	}
	if (!hydrating) return next_sibling;
	if (is_text) {
		if (next_sibling?.nodeType !== 3) {
			var text = create_text();
			if (next_sibling === null) last_sibling?.after(text);
			else next_sibling.before(text);
			set_hydrate_node(text);
			return text;
		}
		merge_text_nodes(next_sibling);
	}
	set_hydrate_node(next_sibling);
	return next_sibling;
}
/**
* @template {Node} N
* @param {N} node
* @returns {void}
*/
function clear_text_content(node) {
	node.textContent = "";
}
/**
* Returns `true` if we're updating the current block, for example `condition` in
* an `{#if condition}` block just changed. In this case, the branch should be
* appended (or removed) at the same time as other updates within the
* current `<svelte:boundary>`
*/
function should_defer_append() {
	if (!async_mode_flag) return false;
	if (eager_block_effects !== null) return false;
	return (active_effect.f & REACTION_RAN) !== 0;
}
/**
* Branching here is intentional and load-bearing for perf. `createElement(tag)`
* hits a fast path in Blink that `createElementNS(NAMESPACE_HTML, tag)` doesn't,
* and passing an explicit `undefined` as the trailing options arg measurably
* slows both APIs. Funnelling every case through a single `createElementNS(ns,
* tag, options)` call would be smaller but slower on the HTML path.
*
* @template {keyof HTMLElementTagNameMap | string} T
* @param {T} tag
* @param {string} [namespace]
* @param {string} [is]
* @returns {T extends keyof HTMLElementTagNameMap ? HTMLElementTagNameMap[T] : Element}
*/
function create_element(tag, namespace, is) {
	if (namespace == null || namespace === "http://www.w3.org/1999/xhtml") return is ? document.createElement(tag, { is }) : document.createElement(tag);
	return is ? document.createElementNS(namespace, tag, { is }) : document.createElementNS(namespace, tag);
}
/**
* Browsers split text nodes larger than 65536 bytes when parsing.
* For hydration to succeed, we need to stitch them back together
* @param {Text} text
*/
function merge_text_nodes(text) {
	if (text.nodeValue.length < 65536) return;
	let next = text.nextSibling;
	while (next !== null && next.nodeType === 3) {
		next.remove();
		/** @type {string} */ text.nodeValue += next.nodeValue;
		next = text.nextSibling;
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/misc.js
var listening_to_form_reset = false;
function add_form_reset_listener() {
	if (!listening_to_form_reset) {
		listening_to_form_reset = true;
		document.addEventListener("reset", (evt) => {
			Promise.resolve().then(() => {
				if (!evt.defaultPrevented) for (const e of evt.target.elements)
 /** @type {any} */ e[FORM_RESET_HANDLER]?.();
			});
		}, { capture: true });
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/bindings/shared.js
/**
* @template T
* @param {() => T} fn
*/
function without_reactive_context(fn) {
	var previous_reaction = active_reaction;
	var previous_effect = active_effect;
	set_active_reaction(null);
	set_active_effect(null);
	try {
		return fn();
	} finally {
		set_active_reaction(previous_reaction);
		set_active_effect(previous_effect);
	}
}
/**
* Listen to the given event, and then instantiate a global form reset listener if not already done,
* to notify all bindings when the form is reset
* @param {HTMLElement} element
* @param {string} event
* @param {(is_reset?: true) => void} handler
* @param {(is_reset?: true) => void} [on_reset]
*/
function listen_to_event_and_reset_event(element, event, handler, on_reset = handler) {
	element.addEventListener(event, () => without_reactive_context(handler));
	const prev = element[FORM_RESET_HANDLER];
	if (prev)
 /** @type {any} */ element[FORM_RESET_HANDLER] = () => {
		prev();
		on_reset(true);
	};
	else
 /** @type {any} */ element[FORM_RESET_HANDLER] = () => on_reset(true);
	add_form_reset_listener();
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/effects.js
/** @import { Blocker, ComponentContext, ComponentContextLegacy, Derived, Effect, TemplateNode, TransitionManager } from '#client' */
/**
* @param {'$effect' | '$effect.pre' | '$inspect'} rune
*/
function validate_effect(rune) {
	if (active_effect === null) {
		if (active_reaction === null) effect_orphan(rune);
		effect_in_unowned_derived();
	}
	if (is_destroying_effect) effect_in_teardown(rune);
}
/**
* @param {Effect} effect
* @param {Effect} parent_effect
*/
function push_effect(effect, parent_effect) {
	var parent_last = parent_effect.last;
	if (parent_last === null) parent_effect.last = parent_effect.first = effect;
	else {
		parent_last.next = effect;
		effect.prev = parent_last;
		parent_effect.last = effect;
	}
}
/**
* @param {number} type
* @param {null | (() => void | (() => void))} fn
* @returns {Effect}
*/
function create_effect(type, fn) {
	var parent = active_effect;
	if (dev_fallback_default) while (parent !== null && (parent.f & 131072) !== 0) parent = parent.parent;
	if (parent !== null && (parent.f & 8192) !== 0) type |= INERT;
	/** @type {Effect} */
	var effect = {
		ctx: component_context,
		deps: null,
		nodes: null,
		f: type | DIRTY | 512,
		first: null,
		fn,
		last: null,
		next: null,
		parent,
		b: parent && parent.b,
		prev: null,
		teardown: null,
		wv: 0,
		ac: null
	};
	if (dev_fallback_default) effect.component_function = dev_current_component_function;
	current_batch?.register_created_effect(effect);
	/** @type {Effect | null} */
	var e = effect;
	if ((type & 4) !== 0) if (collected_effects !== null) collected_effects.push(effect);
	else Batch.ensure().schedule(effect);
	else if (fn !== null) {
		try {
			update_effect(effect);
		} catch (e) {
			destroy_effect(effect);
			throw e;
		}
		if (e.deps === null && e.teardown === null && e.nodes === null && e.first === e.last && (e.f & 524288) === 0) {
			e = e.first;
			if ((type & 16) !== 0 && (type & 65536) !== 0 && e !== null) e.f |= EFFECT_TRANSPARENT;
		}
	}
	if (e !== null) {
		e.parent = parent;
		if (parent !== null) push_effect(e, parent);
		if (active_reaction !== null && (active_reaction.f & 2) !== 0 && (type & 64) === 0) {
			var derived = active_reaction;
			(derived.effects ??= []).push(e);
		}
	}
	return effect;
}
/**
* Internal representation of `$effect.tracking()`
* @returns {boolean}
*/
function effect_tracking() {
	return active_reaction !== null && !untracking;
}
/**
* @param {() => void} fn
*/
function teardown(fn) {
	const effect = create_effect(8, null);
	set_signal_status(effect, CLEAN);
	effect.teardown = fn;
	return effect;
}
/**
* Internal representation of `$effect(...)`
* @param {() => void | (() => void)} fn
*/
function user_effect(fn) {
	validate_effect("$effect");
	if (dev_fallback_default) define_property(fn, "name", { value: "$effect" });
	var flags = active_effect.f;
	if (!active_reaction && (flags & 32) !== 0 && component_context !== null && !component_context.i) {
		var context = component_context;
		(context.e ??= []).push(fn);
	} else return create_user_effect(fn);
}
/**
* @param {() => void | (() => void)} fn
*/
function create_user_effect(fn) {
	return create_effect(4 | USER_EFFECT, fn);
}
/**
* An effect root whose children can transition out
* @param {() => void} fn
* @returns {(options?: { outro?: boolean }) => Promise<void>}
*/
function component_root(fn) {
	Batch.ensure();
	const effect = create_effect(64 | EFFECT_PRESERVED, fn);
	return (options = {}) => {
		return new Promise((fulfil) => {
			if (options.outro) pause_effect(effect, () => {
				destroy_effect(effect);
				fulfil(void 0);
			});
			else {
				destroy_effect(effect);
				fulfil(void 0);
			}
		});
	};
}
/**
* @param {() => void | (() => void)} fn
* @returns {Effect}
*/
function effect(fn) {
	return create_effect(4, fn);
}
/**
* @param {() => void | (() => void)} fn
* @returns {Effect}
*/
function async_effect(fn) {
	return create_effect(ASYNC | EFFECT_PRESERVED, fn);
}
/**
* @param {() => void | (() => void)} fn
* @returns {Effect}
*/
function render_effect(fn, flags = 0) {
	return create_effect(8 | flags, fn);
}
/**
* @param {(...expressions: any) => void | (() => void)} fn
* @param {Array<() => any>} sync
* @param {Array<() => Promise<any>>} async
* @param {Blocker[]} blockers
*/
function template_effect(fn, sync = [], async = [], blockers = []) {
	flatten(blockers, sync, async, (values) => {
		create_effect(8, () => {
			fn(...values.map(get));
		});
	});
}
/**
* @param {(() => void)} fn
* @param {number} flags
*/
function block(fn, flags = 0) {
	var effect = create_effect(16 | flags, fn);
	if (dev_fallback_default) effect.dev_stack = dev_stack;
	return effect;
}
/**
* @param {(() => void)} fn
*/
function branch(fn) {
	return create_effect(32 | EFFECT_PRESERVED, fn);
}
/**
* @param {Effect} effect
*/
function execute_effect_teardown(effect) {
	var teardown = effect.teardown;
	if (teardown !== null) {
		const previously_destroying_effect = is_destroying_effect;
		const previous_reaction = active_reaction;
		set_is_destroying_effect(true);
		set_active_reaction(null);
		try {
			teardown.call(null);
		} finally {
			set_is_destroying_effect(previously_destroying_effect);
			set_active_reaction(previous_reaction);
		}
	}
}
/**
* @param {Effect} signal
* @param {boolean} remove_dom
* @returns {void}
*/
function destroy_effect_children(signal, remove_dom = false) {
	var effect = signal.first;
	signal.first = signal.last = null;
	while (effect !== null) {
		const controller = effect.ac;
		if (controller !== null) without_reactive_context(() => {
			controller.abort(STALE_REACTION);
		});
		var next = effect.next;
		if ((effect.f & 64) !== 0) effect.parent = null;
		else destroy_effect(effect, remove_dom);
		effect = next;
	}
}
/**
* @param {Effect} signal
* @returns {void}
*/
function destroy_block_effect_children(signal) {
	var effect = signal.first;
	while (effect !== null) {
		var next = effect.next;
		if ((effect.f & 32) === 0) destroy_effect(effect);
		effect = next;
	}
}
/**
* @param {Effect} effect
* @param {boolean} [remove_dom]
* @returns {void}
*/
function destroy_effect(effect, remove_dom = true) {
	var removed = false;
	if ((remove_dom || (effect.f & 262144) !== 0) && effect.nodes !== null && effect.nodes.end !== null) {
		remove_effect_dom(effect.nodes.start, effect.nodes.end);
		removed = true;
	}
	effect.f |= DESTROYING;
	destroy_effect_children(effect, remove_dom && !removed);
	remove_reactions(effect, 0);
	var transitions = effect.nodes && effect.nodes.t;
	if (transitions !== null) for (const transition of transitions) transition.stop();
	execute_effect_teardown(effect);
	effect.f ^= DESTROYING;
	effect.f |= DESTROYED;
	var parent = effect.parent;
	if (parent !== null && parent.first !== null) unlink_effect(effect);
	if (dev_fallback_default) effect.component_function = null;
	effect.next = effect.prev = effect.teardown = effect.ctx = effect.deps = effect.fn = effect.nodes = effect.ac = effect.b = null;
}
/**
*
* @param {TemplateNode | null} node
* @param {TemplateNode} end
*/
function remove_effect_dom(node, end) {
	while (node !== null) {
		/** @type {TemplateNode | null} */
		var next = node === end ? null : /* @__PURE__ */ get_next_sibling(node);
		node.remove();
		node = next;
	}
}
/**
* Detach an effect from the effect tree, freeing up memory and
* reducing the amount of work that happens on subsequent traversals
* @param {Effect} effect
*/
function unlink_effect(effect) {
	var parent = effect.parent;
	var prev = effect.prev;
	var next = effect.next;
	if (prev !== null) prev.next = next;
	if (next !== null) next.prev = prev;
	if (parent !== null) {
		if (parent.first === effect) parent.first = next;
		if (parent.last === effect) parent.last = prev;
	}
}
/**
* When a block effect is removed, we don't immediately destroy it or yank it
* out of the DOM, because it might have transitions. Instead, we 'pause' it.
* It stays around (in memory, and in the DOM) until outro transitions have
* completed, and if the state change is reversed then we _resume_ it.
* A paused effect does not update, and the DOM subtree becomes inert.
* @param {Effect} effect
* @param {() => void} [callback]
* @param {boolean} [destroy]
*/
function pause_effect(effect, callback, destroy = true) {
	/** @type {TransitionManager[]} */
	var transitions = [];
	pause_children(effect, transitions, true);
	var fn = () => {
		if (destroy) destroy_effect(effect);
		if (callback) callback();
	};
	var remaining = transitions.length;
	if (remaining > 0) {
		var check = () => --remaining || fn();
		for (var transition of transitions) transition.out(check);
	} else fn();
}
/**
* @param {Effect} effect
* @param {TransitionManager[]} transitions
* @param {boolean} local
*/
function pause_children(effect, transitions, local) {
	if ((effect.f & 8192) !== 0) return;
	effect.f ^= INERT;
	var t = effect.nodes && effect.nodes.t;
	if (t !== null) {
		for (const transition of t) if (transition.is_global || local) transitions.push(transition);
	}
	var child = effect.first;
	while (child !== null) {
		var sibling = child.next;
		if ((child.f & 64) === 0) {
			var transparent = (child.f & 65536) !== 0 || (child.f & 32) !== 0 && (effect.f & 16) !== 0;
			pause_children(child, transitions, transparent ? local : false);
		}
		child = sibling;
	}
}
/**
* The opposite of `pause_effect`. We call this if (for example)
* `x` becomes falsy then truthy: `{#if x}...{/if}`
* @param {Effect} effect
*/
function resume_effect(effect) {
	resume_children(effect, true);
}
/**
* @param {Effect} effect
* @param {boolean} local
*/
function resume_children(effect, local) {
	if ((effect.f & 8192) === 0) return;
	effect.f ^= INERT;
	if ((effect.f & 1024) === 0) {
		set_signal_status(effect, DIRTY);
		Batch.ensure().schedule(effect);
	}
	var child = effect.first;
	while (child !== null) {
		var sibling = child.next;
		var transparent = (child.f & 65536) !== 0 || (child.f & 32) !== 0;
		resume_children(child, transparent ? local : false);
		child = sibling;
	}
	var t = effect.nodes && effect.nodes.t;
	if (t !== null) {
		for (const transition of t) if (transition.is_global || local) transition.in();
	}
}
/**
* @param {Effect} effect
* @param {DocumentFragment} fragment
*/
function move_effect(effect, fragment) {
	if (!effect.nodes) return;
	/** @type {TemplateNode | null} */
	var node = effect.nodes.start;
	var end = effect.nodes.end;
	while (node !== null) {
		/** @type {TemplateNode | null} */
		var next = node === end ? null : /* @__PURE__ */ get_next_sibling(node);
		fragment.append(node);
		node = next;
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/legacy.js
/**
* @type {Set<Value> | null}
* @deprecated
*/
var captured_signals = null;
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/runtime.js
/** @import { Derived, Effect, Reaction, Source, Value } from '#client' */
var is_updating_effect = false;
var is_destroying_effect = false;
/** @param {boolean} value */
function set_is_destroying_effect(value) {
	is_destroying_effect = value;
}
/** @type {null | Reaction} */
var active_reaction = null;
var untracking = false;
/** @param {null | Reaction} reaction */
function set_active_reaction(reaction) {
	active_reaction = reaction;
}
/** @type {null | Effect} */
var active_effect = null;
/** @param {null | Effect} effect */
function set_active_effect(effect) {
	active_effect = effect;
}
/**
* When sources are created within a reaction, reading and writing
* them within that reaction should not cause a re-run
* @type {null | Set<Source>}
*/
var current_sources = null;
/** @param {Value} value */
function push_reaction_value(value) {
	if (active_reaction !== null && (!async_mode_flag || (active_reaction.f & 2) !== 0)) (current_sources ??= /* @__PURE__ */ new Set()).add(value);
}
/**
* The dependencies of the reaction that is currently being executed. In many cases,
* the dependencies are unchanged between runs, and so this will be `null` unless
* and until a new dependency is accessed — we track this via `skipped_deps`
* @type {null | Value[]}
*/
var new_deps = null;
var skipped_deps = 0;
/**
* Tracks writes that the effect it's executed in doesn't listen to yet,
* so that the dependency can be added to the effect later on if it then reads it
* @type {null | Source[]}
*/
var untracked_writes = null;
/** @param {null | Source[]} value */
function set_untracked_writes(value) {
	untracked_writes = value;
}
/**
* @type {number} Used by sources and deriveds for handling updates.
* Version starts from 1 so that unowned deriveds differentiate between a created effect and a run one for tracing
**/
var write_version = 1;
/** @type {number} Used to version each read of a source of derived to avoid duplicating depedencies inside a reaction */
var read_version = 0;
var update_version = read_version;
/** @param {number} value */
function set_update_version(value) {
	update_version = value;
}
function increment_write_version() {
	return ++write_version;
}
/**
* Determines whether a derived or effect is dirty.
* If it is MAYBE_DIRTY, will set the status to CLEAN
* @param {Reaction} reaction
* @returns {boolean}
*/
function is_dirty(reaction) {
	var flags = reaction.f;
	if ((flags & 2048) !== 0) return true;
	if (flags & 2) reaction.f &= ~WAS_MARKED;
	if ((flags & 4096) !== 0) {
		var dependencies = reaction.deps;
		var length = dependencies.length;
		for (var i = 0; i < length; i++) {
			var dependency = dependencies[i];
			if (is_dirty(dependency)) update_derived(dependency);
			if (dependency.wv > reaction.wv) return true;
		}
		if ((flags & 512) !== 0 && batch_values === null) set_signal_status(reaction, CLEAN);
	}
	return false;
}
/**
* @param {Value} signal
* @param {Effect} effect
* @param {boolean} [root]
*/
function schedule_possible_effect_self_invalidation(signal, effect, root = true) {
	var reactions = signal.reactions;
	if (reactions === null) return;
	if (!async_mode_flag && current_sources !== null && current_sources.has(signal)) return;
	for (var i = 0; i < reactions.length; i++) {
		var reaction = reactions[i];
		if ((reaction.f & 2) !== 0) schedule_possible_effect_self_invalidation(reaction, effect, false);
		else if (effect === reaction) {
			if (root) set_signal_status(reaction, DIRTY);
			else if ((reaction.f & 1024) !== 0) set_signal_status(reaction, MAYBE_DIRTY);
			schedule_effect(reaction);
		}
	}
}
/** @param {Reaction} reaction */
function update_reaction(reaction) {
	var previous_deps = new_deps;
	var previous_skipped_deps = skipped_deps;
	var previous_untracked_writes = untracked_writes;
	var previous_reaction = active_reaction;
	var previous_sources = current_sources;
	var previous_component_context = component_context;
	var previous_untracking = untracking;
	var previous_update_version = update_version;
	var flags = reaction.f;
	new_deps = null;
	skipped_deps = 0;
	untracked_writes = null;
	active_reaction = (flags & 96) === 0 ? reaction : null;
	current_sources = null;
	set_component_context(reaction.ctx);
	untracking = false;
	update_version = ++read_version;
	if (reaction.ac !== null) {
		without_reactive_context(() => {
			/** @type {AbortController} */ reaction.ac.abort(STALE_REACTION);
		});
		reaction.ac = null;
	}
	try {
		reaction.f |= REACTION_IS_UPDATING;
		var fn = reaction.fn;
		var result = fn();
		reaction.f |= REACTION_RAN;
		var deps = reaction.deps;
		var is_fork = current_batch?.is_fork;
		if (new_deps !== null) {
			var i;
			if (!is_fork) remove_reactions(reaction, skipped_deps);
			if (deps !== null && skipped_deps > 0) {
				deps.length = skipped_deps + new_deps.length;
				for (i = 0; i < new_deps.length; i++) deps[skipped_deps + i] = new_deps[i];
			} else reaction.deps = deps = new_deps;
			if (effect_tracking() && (reaction.f & 512) !== 0) for (i = skipped_deps; i < deps.length; i++) (deps[i].reactions ??= []).push(reaction);
		} else if (!is_fork && deps !== null && skipped_deps < deps.length) {
			remove_reactions(reaction, skipped_deps);
			deps.length = skipped_deps;
		}
		if (is_runes() && untracked_writes !== null && !untracking && deps !== null && (reaction.f & 6146) === 0) for (i = 0; i < untracked_writes.length; i++) schedule_possible_effect_self_invalidation(untracked_writes[i], reaction);
		if (previous_reaction !== null && previous_reaction !== reaction) {
			read_version++;
			if (previous_reaction.deps !== null) for (let i = 0; i < previous_skipped_deps; i += 1) previous_reaction.deps[i].rv = read_version;
			if (previous_deps !== null) for (const dep of previous_deps) dep.rv = read_version;
			if (untracked_writes !== null) if (previous_untracked_writes === null) previous_untracked_writes = untracked_writes;
			else previous_untracked_writes.push(...untracked_writes);
		}
		if ((reaction.f & 8388608) !== 0) reaction.f ^= ERROR_VALUE;
		return result;
	} catch (error) {
		return handle_error(error);
	} finally {
		reaction.f ^= REACTION_IS_UPDATING;
		new_deps = previous_deps;
		skipped_deps = previous_skipped_deps;
		untracked_writes = previous_untracked_writes;
		active_reaction = previous_reaction;
		current_sources = previous_sources;
		set_component_context(previous_component_context);
		untracking = previous_untracking;
		update_version = previous_update_version;
	}
}
/**
* @template V
* @param {Reaction} signal
* @param {Value<V>} dependency
* @returns {void}
*/
function remove_reaction(signal, dependency) {
	let reactions = dependency.reactions;
	if (reactions !== null) {
		var index = index_of.call(reactions, signal);
		if (index !== -1) {
			var new_length = reactions.length - 1;
			if (new_length === 0) reactions = dependency.reactions = null;
			else {
				reactions[index] = reactions[new_length];
				reactions.pop();
			}
		}
	}
	if (reactions === null && (dependency.f & 2) !== 0 && (new_deps === null || !includes.call(new_deps, dependency))) {
		var derived = dependency;
		if ((derived.f & 512) !== 0) {
			derived.f ^= 512;
			derived.f &= ~WAS_MARKED;
		}
		if (derived.v !== UNINITIALIZED) update_derived_status(derived);
		freeze_derived_effects(derived);
		remove_reactions(derived, 0);
	}
}
/**
* @param {Reaction} signal
* @param {number} start_index
* @returns {void}
*/
function remove_reactions(signal, start_index) {
	var dependencies = signal.deps;
	if (dependencies === null) return;
	for (var i = start_index; i < dependencies.length; i++) remove_reaction(signal, dependencies[i]);
}
/**
* @param {Effect} effect
* @returns {void}
*/
function update_effect(effect) {
	var flags = effect.f;
	if ((flags & 16384) !== 0) return;
	set_signal_status(effect, CLEAN);
	var previous_effect = active_effect;
	var was_updating_effect = is_updating_effect;
	active_effect = effect;
	is_updating_effect = true;
	if (dev_fallback_default) {
		var previous_component_fn = dev_current_component_function;
		set_dev_current_component_function(effect.component_function);
		var previous_stack = dev_stack;
		set_dev_stack(effect.dev_stack ?? dev_stack);
	}
	try {
		if ((flags & 16777232) !== 0) destroy_block_effect_children(effect);
		else destroy_effect_children(effect);
		execute_effect_teardown(effect);
		var teardown = update_reaction(effect);
		effect.teardown = typeof teardown === "function" ? teardown : null;
		effect.wv = write_version;
		if (dev_fallback_default && tracing_mode_flag && (effect.f & 2048) !== 0 && effect.deps !== null) {
			for (var dep of effect.deps) if (dep.set_during_effect) {
				dep.wv = increment_write_version();
				dep.set_during_effect = false;
			}
		}
	} finally {
		is_updating_effect = was_updating_effect;
		active_effect = previous_effect;
		if (dev_fallback_default) {
			set_dev_current_component_function(previous_component_fn);
			set_dev_stack(previous_stack);
		}
	}
}
/**
* Returns a promise that resolves once any pending state changes have been applied.
* @returns {Promise<void>}
*/
async function tick() {
	if (async_mode_flag) return new Promise((f) => {
		requestAnimationFrame(() => f());
		setTimeout(() => f());
	});
	await Promise.resolve();
	flushSync();
}
/**
* @template V
* @param {Value<V>} signal
* @returns {V}
*/
function get(signal) {
	var is_derived = (signal.f & 2) !== 0;
	captured_signals?.add(signal);
	if (active_reaction !== null && !untracking) {
		if (!(active_effect !== null && (active_effect.f & 16384) !== 0) && (current_sources === null || !current_sources.has(signal))) {
			var deps = active_reaction.deps;
			if ((active_reaction.f & 2097152) !== 0) {
				if (signal.rv < read_version) {
					signal.rv = read_version;
					if (new_deps === null && deps !== null && deps[skipped_deps] === signal) skipped_deps++;
					else if (new_deps === null) new_deps = [signal];
					else new_deps.push(signal);
				}
			} else {
				active_reaction.deps ??= [];
				if (!includes.call(active_reaction.deps, signal)) active_reaction.deps.push(signal);
				var reactions = signal.reactions;
				if (reactions === null) signal.reactions = [active_reaction];
				else if (!includes.call(reactions, active_reaction)) reactions.push(active_reaction);
			}
		}
	}
	if (dev_fallback_default) {
		if (!untracking && reactivity_loss_tracker && current_batch === null && previous_batch === null && !reactivity_loss_tracker.warned && (reactivity_loss_tracker.effect.f & 2097152) === 0 && !reactivity_loss_tracker.effect_deps.has(signal)) {
			reactivity_loss_tracker.warned = true;
			await_reactivity_loss(signal.label);
			var trace = get_error("traced at");
			if (trace) console.warn(trace);
		}
		recent_async_deriveds.delete(signal);
		if (tracing_mode_flag && !untracking && tracing_expressions !== null && active_reaction !== null && tracing_expressions.reaction === active_reaction) if (signal.trace) signal.trace();
		else {
			trace = get_error("traced at");
			if (trace) {
				var entry = tracing_expressions.entries.get(signal);
				if (entry === void 0) {
					entry = { traces: [] };
					tracing_expressions.entries.set(signal, entry);
				}
				var last = entry.traces[entry.traces.length - 1];
				if (trace.stack !== last?.stack) entry.traces.push(trace);
			}
		}
	}
	if (is_destroying_effect && old_values.has(signal)) return old_values.get(signal);
	if (is_derived) {
		var derived = signal;
		if (is_destroying_effect) {
			var value = derived.v;
			if ((derived.f & 1024) === 0 && derived.reactions !== null || depends_on_old_values(derived)) value = execute_derived(derived);
			old_values.set(derived, value);
			return value;
		}
		var should_connect = (derived.f & 512) === 0 && !untracking && active_reaction !== null && (is_updating_effect || (active_reaction.f & 512) !== 0);
		var is_new = (derived.f & REACTION_RAN) === 0;
		if (is_dirty(derived)) {
			if (should_connect) derived.f |= 512;
			update_derived(derived);
		}
		if (should_connect && !is_new) {
			unfreeze_derived_effects(derived);
			reconnect(derived);
		}
	}
	if (batch_values?.has(signal)) return batch_values.get(signal);
	if ((signal.f & 8388608) !== 0) throw signal.v;
	return signal.v;
}
/**
* (Re)connect a disconnected derived, so that it is notified
* of changes in `mark_reactions`
* @param {Derived} derived
*/
function reconnect(derived) {
	derived.f |= 512;
	if (derived.deps === null) return;
	for (const dep of derived.deps) {
		(dep.reactions ??= []).push(derived);
		if ((dep.f & 2) !== 0 && (dep.f & 512) === 0) {
			unfreeze_derived_effects(dep);
			reconnect(dep);
		}
	}
}
/** @param {Derived} derived */
function depends_on_old_values(derived) {
	if (derived.v === UNINITIALIZED) return true;
	if (derived.deps === null) return false;
	for (const dep of derived.deps) {
		if (old_values.has(dep)) return true;
		if ((dep.f & 2) !== 0 && depends_on_old_values(dep)) return true;
	}
	return false;
}
/**
* When used inside a [`$derived`](https://svelte.dev/docs/svelte/$derived) or [`$effect`](https://svelte.dev/docs/svelte/$effect),
* any state read inside `fn` will not be treated as a dependency.
*
* ```ts
* $effect(() => {
*   // this will run when `data` changes, but not when `time` changes
*   save(data, {
*     timestamp: untrack(() => time)
*   });
* });
* ```
* @template T
* @param {() => T} fn
* @returns {T}
*/
function untrack(fn) {
	var previous_untracking = untracking;
	try {
		untracking = true;
		return fn();
	} finally {
		untracking = previous_untracking;
	}
}
/**
* Subset of delegated events which should be passive by default.
* These two are already passive via browser defaults on window, document and body.
* But since
* - we're delegating them
* - they happen often
* - they apply to mobile which is generally less performant
* we're marking them as passive by default for other elements, too.
*/
var PASSIVE_EVENTS = ["touchstart", "touchmove"];
/**
* Returns `true` if `name` is a passive event
* @param {string} name
*/
function is_passive_event(name) {
	return PASSIVE_EVENTS.includes(name);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/events.js
/**
* Used on elements, as a map of event type -> event handler,
* and on events themselves to track which element handled an event
*/
var event_symbol = Symbol("events");
/** @type {Set<string>} */
var all_registered_events = /* @__PURE__ */ new Set();
/** @type {Set<(events: Array<string>) => void>} */
var root_event_handles = /* @__PURE__ */ new Set();
/**
* @param {string} event_name
* @param {Element} element
* @param {EventListener} [handler]
* @returns {void}
*/
function delegated(event_name, element, handler) {
	(element[event_symbol] ??= {})[event_name] = handler;
}
/**
* @param {Array<string>} events
* @returns {void}
*/
function delegate(events) {
	for (var i = 0; i < events.length; i++) all_registered_events.add(events[i]);
	for (var fn of root_event_handles) fn(events);
}
var last_propagated_event = null;
/**
* @this {EventTarget}
* @param {Event} event
* @returns {void}
*/
function handle_event_propagation(event) {
	var handler_element = this;
	var owner_document = handler_element.ownerDocument;
	var event_name = event.type;
	var path = event.composedPath?.() || [];
	var current_target = path[0] || event.target;
	last_propagated_event = event;
	var path_idx = 0;
	var handled_at = last_propagated_event === event && event[event_symbol];
	if (handled_at) {
		var at_idx = path.indexOf(handled_at);
		if (at_idx !== -1 && (handler_element === document || handler_element === window)) {
			event[event_symbol] = handler_element;
			return;
		}
		var handler_idx = path.indexOf(handler_element);
		if (handler_idx === -1) return;
		if (at_idx <= handler_idx) path_idx = at_idx;
	}
	current_target = path[path_idx] || event.target;
	if (current_target === handler_element) return;
	define_property(event, "currentTarget", {
		configurable: true,
		get() {
			return current_target || owner_document;
		}
	});
	var previous_reaction = active_reaction;
	var previous_effect = active_effect;
	set_active_reaction(null);
	set_active_effect(null);
	try {
		/**
		* @type {unknown}
		*/
		var throw_error;
		/**
		* @type {unknown[]}
		*/
		var other_errors = [];
		while (current_target !== null) {
			if (current_target === handler_element) break;
			try {
				var delegated = current_target[event_symbol]?.[event_name];
				if (delegated != null && (!current_target.disabled || event.target === current_target)) delegated.call(current_target, event);
			} catch (error) {
				if (throw_error) other_errors.push(error);
				else throw_error = error;
			}
			if (event.cancelBubble) break;
			path_idx++;
			current_target = path_idx < path.length ? path[path_idx] : null;
		}
		if (throw_error) {
			for (let error of other_errors) queueMicrotask(() => {
				throw error;
			});
			throw throw_error;
		}
	} finally {
		event[event_symbol] = handler_element;
		delete event.currentTarget;
		set_active_reaction(previous_reaction);
		set_active_effect(previous_effect);
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/reconciler.js
var policy = globalThis?.window?.trustedTypes && /* @__PURE__ */ globalThis.window.trustedTypes.createPolicy("svelte-trusted-html", {
/** @param {string} html */
createHTML: (html) => {
	return html;
} });
/** @param {string} html */
function create_trusted_html(html) {
	return policy?.createHTML(html) ?? html;
}
/**
* @param {string} html
*/
function create_fragment_from_html(html) {
	var elem = create_element("template");
	elem.innerHTML = create_trusted_html(html.replaceAll("<!>", "<!---->"));
	return elem.content;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/template.js
/** @import { Effect, EffectNodes, TemplateNode } from '#client' */
/** @import { TemplateStructure } from './types' */
/**
* @param {TemplateNode} start
* @param {TemplateNode | null} end
*/
function assign_nodes(start, end) {
	var effect = active_effect;
	if (effect.nodes === null) effect.nodes = {
		start,
		end,
		a: null,
		t: null
	};
}
/**
* @param {string} content
* @param {number} flags
* @returns {() => Node | Node[]}
*/
/*#__NO_SIDE_EFFECTS__*/
function from_html(content, flags) {
	var is_fragment = (flags & 1) !== 0;
	var use_import_node = (flags & 2) !== 0;
	/** @type {Node} */
	var node;
	/**
	* Whether or not the first item is a text/element node. If not, we need to
	* create an additional comment node to act as `effect.nodes.start`
	*/
	var has_start = !content.startsWith("<!>");
	return () => {
		if (hydrating) {
			assign_nodes(hydrate_node, null);
			return hydrate_node;
		}
		if (node === void 0) {
			node = create_fragment_from_html(has_start ? content : "<!>" + content);
			if (!is_fragment) node = /* @__PURE__ */ get_first_child(node);
		}
		var clone = use_import_node || is_firefox ? document.importNode(node, true) : node.cloneNode(true);
		if (is_fragment) {
			var start = /* @__PURE__ */ get_first_child(clone);
			var end = clone.lastChild;
			assign_nodes(start, end);
		} else assign_nodes(clone, clone);
		return clone;
	};
}
/**
* @returns {TemplateNode | DocumentFragment}
*/
function comment() {
	if (hydrating) {
		assign_nodes(hydrate_node, null);
		return hydrate_node;
	}
	var frag = document.createDocumentFragment();
	var start = document.createComment("");
	var anchor = create_text();
	frag.append(start, anchor);
	assign_nodes(start, anchor);
	return frag;
}
/**
* Assign the created (or in hydration mode, traversed) dom elements to the current block
* and insert the elements into the dom (in client mode).
* @param {Text | Comment | Element} anchor
* @param {DocumentFragment | Element} dom
*/
function append(anchor, dom) {
	if (hydrating) {
		var effect = active_effect;
		if ((effect.f & 32768) === 0 || effect.nodes.end === null) effect.nodes.end = hydrate_node;
		hydrate_next();
		return;
	}
	if (anchor === null) return;
	anchor.before(dom);
}
/**
* @param {Element} text
* @param {string} value
* @returns {void}
*/
function set_text(text, value) {
	var str = value == null ? "" : typeof value === "object" ? `${value}` : value;
	if (str !== (text[TEXT_CACHE] ??= text.nodeValue)) {
		/** @type {any} */ text[TEXT_CACHE] = str;
		text.nodeValue = `${str}`;
	}
}
/**
* Mounts a component to the given target and returns the exports and potentially the props (if compiled with `accessors: true`) of the component.
* Transitions will play during the initial render unless the `intro` option is set to `false`.
*
* @template {Record<string, any>} Props
* @template {Record<string, any>} Exports
* @param {ComponentType<SvelteComponent<Props>> | Component<Props, Exports, any>} component
* @param {MountOptions<Props>} options
* @returns {Exports}
*/
function mount(component, options) {
	return _mount(component, options);
}
/** @type {Map<EventTarget, Map<string, number>>} */
var listeners = /* @__PURE__ */ new Map();
/**
* @template {Record<string, any>} Exports
* @param {ComponentType<SvelteComponent<any>> | Component<any>} Component
* @param {MountOptions} options
* @returns {Exports}
*/
function _mount(Component, { target, anchor, props = {}, events, context, intro = true, transformError }) {
	init_operations();
	/** @type {Exports} */
	var component = void 0;
	var unmount = component_root(() => {
		var anchor_node = anchor ?? target.appendChild(create_text());
		boundary(anchor_node, { pending: () => {} }, (anchor_node) => {
			push({});
			var ctx = component_context;
			if (context) ctx.c = context;
			if (events)
 /** @type {any} */ props.$$events = events;
			if (hydrating) assign_nodes(anchor_node, null);
			component = Component(anchor_node, props) || {};
			if (hydrating) {
				/** @type {Effect & { nodes: EffectNodes }} */ active_effect.nodes.end = hydrate_node;
				if (hydrate_node === null || hydrate_node.nodeType !== 8 || hydrate_node.data !== "]") {
					hydration_mismatch();
					throw HYDRATION_ERROR;
				}
			}
			pop();
		}, transformError);
		/** @type {Set<string>} */
		var registered_events = /* @__PURE__ */ new Set();
		/** @param {Array<string>} events */
		var event_handle = (events) => {
			for (var i = 0; i < events.length; i++) {
				var event_name = events[i];
				if (registered_events.has(event_name)) continue;
				registered_events.add(event_name);
				var passive = is_passive_event(event_name);
				for (const node of [target, document]) {
					var counts = listeners.get(node);
					if (counts === void 0) {
						counts = /* @__PURE__ */ new Map();
						listeners.set(node, counts);
					}
					var count = counts.get(event_name);
					if (count === void 0) {
						node.addEventListener(event_name, handle_event_propagation, { passive });
						counts.set(event_name, 1);
					} else counts.set(event_name, count + 1);
				}
			}
		};
		event_handle(array_from(all_registered_events));
		root_event_handles.add(event_handle);
		return () => {
			for (var event_name of registered_events) for (const node of [target, document]) {
				var counts = listeners.get(node);
				var count = counts.get(event_name);
				if (--count == 0) {
					node.removeEventListener(event_name, handle_event_propagation);
					counts.delete(event_name);
					if (counts.size === 0) listeners.delete(node);
				} else counts.set(event_name, count);
			}
			root_event_handles.delete(event_handle);
			if (anchor_node !== anchor) anchor_node.parentNode?.removeChild(anchor_node);
		};
	});
	mounted_components.set(component, unmount);
	return component;
}
/**
* References of the components that were mounted or hydrated.
* Uses a `WeakMap` to avoid memory leaks.
*/
var mounted_components = /* @__PURE__ */ new WeakMap();
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/blocks/branches.js
/** @import { Effect, TemplateNode } from '#client' */
/**
* @typedef {{ effect: Effect, fragment: DocumentFragment }} Branch
*/
/**
* @template Key
*/
var BranchManager = class {
	/** @type {TemplateNode} */
	anchor;
	/** @type {Map<Batch, Key>} */
	#batches = /* @__PURE__ */ new Map();
	/**
	* Map of keys to effects that are currently rendered in the DOM.
	* These effects are visible and actively part of the document tree.
	* Example:
	* ```
	* {#if condition}
	* 	foo
	* {:else}
	* 	bar
	* {/if}
	* ```
	* Can result in the entries `true->Effect` and `false->Effect`
	* @type {Map<Key, Effect>}
	*/
	#onscreen = /* @__PURE__ */ new Map();
	/**
	* Similar to #onscreen with respect to the keys, but contains branches that are not yet
	* in the DOM, because their insertion is deferred.
	* @type {Map<Key, Branch>}
	*/
	#offscreen = /* @__PURE__ */ new Map();
	/**
	* Keys of effects that are currently outroing
	* @type {Set<Key>}
	*/
	#outroing = /* @__PURE__ */ new Set();
	/**
	* Whether to pause (i.e. outro) on change, or destroy immediately.
	* This is necessary for `<svelte:element>`
	*/
	#transition = true;
	/**
	* @param {TemplateNode} anchor
	* @param {boolean} transition
	*/
	constructor(anchor, transition = true) {
		this.anchor = anchor;
		this.#transition = transition;
	}
	/**
	* @param {Batch} batch
	*/
	#commit = (batch) => {
		if (!this.#batches.has(batch)) return;
		var key = this.#batches.get(batch);
		var onscreen = this.#onscreen.get(key);
		if (onscreen) {
			resume_effect(onscreen);
			this.#outroing.delete(key);
		} else {
			var offscreen = this.#offscreen.get(key);
			if (offscreen) {
				resume_effect(offscreen.effect);
				this.#onscreen.set(key, offscreen.effect);
				this.#offscreen.delete(key);
				if (dev_fallback_default)
 /** @type {any} */ offscreen.fragment.lastChild[HMR_ANCHOR] = this.anchor;
				/** @type {TemplateNode} */ offscreen.fragment.lastChild.remove();
				this.anchor.before(offscreen.fragment);
				onscreen = offscreen.effect;
			}
		}
		for (const [b, k] of this.#batches) {
			this.#batches.delete(b);
			if (b === batch) break;
			const offscreen = this.#offscreen.get(k);
			if (offscreen) {
				destroy_effect(offscreen.effect);
				this.#offscreen.delete(k);
			}
		}
		for (const [k, effect] of this.#onscreen) {
			if (k === key || this.#outroing.has(k)) continue;
			const on_destroy = () => {
				if (Array.from(this.#batches.values()).includes(k)) {
					var fragment = document.createDocumentFragment();
					move_effect(effect, fragment);
					fragment.append(create_text());
					this.#offscreen.set(k, {
						effect,
						fragment
					});
				} else destroy_effect(effect);
				this.#outroing.delete(k);
				this.#onscreen.delete(k);
			};
			if (this.#transition || !onscreen) {
				this.#outroing.add(k);
				pause_effect(effect, on_destroy, false);
			} else on_destroy();
		}
	};
	/**
	* @param {Batch} batch
	*/
	#discard = (batch) => {
		this.#batches.delete(batch);
		const keys = Array.from(this.#batches.values());
		for (const [k, branch] of this.#offscreen) if (!keys.includes(k)) {
			destroy_effect(branch.effect);
			this.#offscreen.delete(k);
		}
	};
	/**
	*
	* @param {any} key
	* @param {null | ((target: TemplateNode) => void)} fn
	*/
	ensure(key, fn) {
		var batch = current_batch;
		var defer = should_defer_append();
		if (fn && !this.#onscreen.has(key) && !this.#offscreen.has(key)) if (defer) {
			var fragment = document.createDocumentFragment();
			var target = create_text();
			fragment.append(target);
			this.#offscreen.set(key, {
				effect: branch(() => fn(target)),
				fragment
			});
		} else this.#onscreen.set(key, branch(() => fn(this.anchor)));
		this.#batches.set(batch, key);
		if (defer) {
			for (const [k, effect] of this.#onscreen) if (k === key) batch.unskip_effect(effect);
			else batch.skip_effect(effect);
			for (const [k, branch] of this.#offscreen) if (k === key) batch.unskip_effect(branch.effect);
			else batch.skip_effect(branch.effect);
			batch.oncommit(this.#commit);
			batch.ondiscard(this.#discard);
		} else {
			if (hydrating) this.anchor = hydrate_node;
			this.#commit(batch);
		}
	}
};
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/blocks/if.js
/** @import { TemplateNode } from '#client' */
/**
* @param {TemplateNode} node
* @param {(branch: (fn: (anchor: Node) => void, key?: number | false) => void) => void} fn
* @param {boolean} [elseif] True if this is an `{:else if ...}` block rather than an `{#if ...}`, as that affects which transitions are considered 'local'
* @returns {void}
*/
function if_block(node, fn, elseif = false) {
	/** @type {TemplateNode | undefined} */
	var marker;
	if (hydrating) {
		marker = hydrate_node;
		hydrate_next();
	}
	var branches = new BranchManager(node);
	var flags = elseif ? EFFECT_TRANSPARENT : 0;
	/**
	* @param {number | false} key
	* @param {null | ((anchor: Node) => void)} fn
	*/
	function update_branch(key, fn) {
		if (hydrating) {
			var data = read_hydration_instruction(marker);
			if (key !== parseInt(data.substring(1))) {
				var anchor = skip_nodes();
				set_hydrate_node(anchor);
				branches.anchor = anchor;
				set_hydrating(false);
				branches.ensure(key, fn);
				set_hydrating(true);
				return;
			}
		}
		branches.ensure(key, fn);
	}
	block(() => {
		var has_branch = false;
		fn((fn, key = 0) => {
			has_branch = true;
			update_branch(key, fn);
		});
		if (!has_branch) update_branch(-1, null);
	}, flags);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/blocks/each.js
/** @import { EachItem, EachOutroGroup, EachState, Effect, EffectNodes, MaybeSource, Source, TemplateNode, TransitionManager, Value } from '#client' */
/** @import { Batch } from '../../reactivity/batch.js'; */
/**
* @param {any} _
* @param {number} i
*/
function index(_, i) {
	return i;
}
/**
* Pause multiple effects simultaneously, and coordinate their
* subsequent destruction. Used in each blocks
* @param {EachState} state
* @param {Effect[]} to_destroy
* @param {null | Node} controlled_anchor
*/
function pause_effects(state, to_destroy, controlled_anchor) {
	/** @type {TransitionManager[]} */
	var transitions = [];
	var length = to_destroy.length;
	/** @type {EachOutroGroup} */
	var group;
	var remaining = to_destroy.length;
	for (var i = 0; i < length; i++) {
		let effect = to_destroy[i];
		pause_effect(effect, () => {
			if (group) {
				group.pending.delete(effect);
				group.done.add(effect);
				if (group.pending.size === 0) {
					var groups = state.outrogroups;
					destroy_effects(state, array_from(group.done));
					groups.delete(group);
					if (groups.size === 0) state.outrogroups = null;
				}
			} else remaining -= 1;
		}, false);
	}
	if (remaining === 0) {
		var fast_path = transitions.length === 0 && controlled_anchor !== null;
		if (fast_path) {
			var anchor = controlled_anchor;
			var parent_node = anchor.parentNode;
			clear_text_content(parent_node);
			parent_node.append(anchor);
			state.items.clear();
		}
		destroy_effects(state, to_destroy, !fast_path);
	} else {
		group = {
			pending: new Set(to_destroy),
			done: /* @__PURE__ */ new Set()
		};
		(state.outrogroups ??= /* @__PURE__ */ new Set()).add(group);
	}
}
/**
* @param {EachState} state
* @param {Effect[]} to_destroy
* @param {boolean} remove_dom
*/
function destroy_effects(state, to_destroy, remove_dom = true) {
	/** @type {Set<Effect> | undefined} */
	var preserved_effects;
	if (state.pending.size > 0) {
		preserved_effects = /* @__PURE__ */ new Set();
		for (const keys of state.pending.values()) for (const key of keys) preserved_effects.add(
			/** @type {EachItem} */
			state.items.get(key).e
		);
	}
	for (var i = 0; i < to_destroy.length; i++) {
		var e = to_destroy[i];
		if (preserved_effects?.has(e)) {
			e.f |= EFFECT_OFFSCREEN;
			move_effect(e, document.createDocumentFragment());
		} else destroy_effect(to_destroy[i], remove_dom);
	}
}
/** @type {TemplateNode} */
var offscreen_anchor;
/**
* @template V
* @param {Element | Comment} node The next sibling node, or the parent node if this is a 'controlled' block
* @param {number} flags
* @param {() => V[]} get_collection
* @param {(value: V, index: number) => any} get_key
* @param {(anchor: Node, item: MaybeSource<V>, index: MaybeSource<number>) => void} render_fn
* @param {null | ((anchor: Node) => void)} fallback_fn
* @returns {void}
*/
function each(node, flags, get_collection, get_key, render_fn, fallback_fn = null) {
	var anchor = node;
	/** @type {Map<any, EachItem>} */
	var items = /* @__PURE__ */ new Map();
	if ((flags & 4) !== 0) {
		var parent_node = node;
		anchor = hydrating ? set_hydrate_node(/* @__PURE__ */ get_first_child(parent_node)) : parent_node.appendChild(create_text());
	}
	if (hydrating) hydrate_next();
	/** @type {Effect | null} */
	var fallback = null;
	var each_array = /* @__PURE__ */ derived_safe_equal(() => {
		var collection = get_collection();
		return is_array(collection) ? collection : collection == null ? [] : array_from(collection);
	});
	if (dev_fallback_default) tag(each_array, "{#each ...}");
	/** @type {V[]} */
	var array;
	/** @type {Map<Batch, Set<any>>} */
	var pending = /* @__PURE__ */ new Map();
	var first_run = true;
	/**
	* @param {Batch} batch
	*/
	function commit(batch) {
		if ((state.effect.f & 16384) !== 0) return;
		state.pending.delete(batch);
		state.fallback = fallback;
		reconcile(state, array, anchor, flags, get_key);
		if (fallback !== null) if (array.length === 0) if ((fallback.f & 33554432) === 0) resume_effect(fallback);
		else {
			fallback.f ^= EFFECT_OFFSCREEN;
			move(fallback, null, anchor);
		}
		else pause_effect(fallback, () => {
			fallback = null;
		});
	}
	/**
	* @param {Batch} batch
	*/
	function discard(batch) {
		state.pending.delete(batch);
	}
	/** @type {EachState} */
	var state = {
		effect: block(() => {
			array = get(each_array);
			var length = array.length;
			/** `true` if there was a hydration mismatch. Needs to be a `let` or else it isn't treeshaken out */
			let mismatch = false;
			if (hydrating) {
				if (read_hydration_instruction(anchor) === "[!" !== (length === 0)) {
					anchor = skip_nodes();
					set_hydrate_node(anchor);
					set_hydrating(false);
					mismatch = true;
				}
			}
			var keys = /* @__PURE__ */ new Set();
			var batch = current_batch;
			var defer = should_defer_append();
			for (var index = 0; index < length; index += 1) {
				if (hydrating && hydrate_node.nodeType === 8 && hydrate_node.data === "]") {
					anchor = hydrate_node;
					mismatch = true;
					set_hydrating(false);
				}
				var value = array[index];
				var key = get_key(value, index);
				if (dev_fallback_default) {
					var key_again = get_key(value, index);
					if (key !== key_again) each_key_volatile(String(index), String(key), String(key_again));
				}
				var item = first_run ? null : items.get(key);
				if (item) {
					if (item.v) internal_set(item.v, value);
					if (item.i) internal_set(item.i, index);
					if (defer) batch.unskip_effect(item.e);
				} else {
					item = create_item(items, first_run ? anchor : offscreen_anchor ??= create_text(), value, key, index, render_fn, flags, get_collection);
					if (!first_run) item.e.f |= EFFECT_OFFSCREEN;
					items.set(key, item);
				}
				keys.add(key);
			}
			if (length === 0 && fallback_fn && !fallback) if (first_run) fallback = branch(() => fallback_fn(anchor));
			else {
				fallback = branch(() => fallback_fn(offscreen_anchor ??= create_text()));
				fallback.f |= EFFECT_OFFSCREEN;
			}
			if (length > keys.size) if (dev_fallback_default) validate_each_keys(array, get_key);
			else each_key_duplicate("", "", "");
			if (hydrating && length > 0) set_hydrate_node(skip_nodes());
			if (!first_run) {
				pending.set(batch, keys);
				if (defer) {
					for (const [key, item] of items) if (!keys.has(key)) batch.skip_effect(item.e);
					batch.oncommit(commit);
					batch.ondiscard(discard);
				} else commit(batch);
			}
			if (mismatch) set_hydrating(true);
			get(each_array);
		}),
		flags,
		items,
		pending,
		outrogroups: null,
		fallback
	};
	first_run = false;
	if (hydrating) anchor = hydrate_node;
}
/**
* Skip past any non-branch effects (which could be created with `createSubscriber`, for example) to find the next branch effect
* @param {Effect | null} effect
* @returns {Effect | null}
*/
function skip_to_branch(effect) {
	while (effect !== null && (effect.f & 32) === 0) effect = effect.next;
	return effect;
}
/**
* Add, remove, or reorder items output by an each block as its input changes
* @template V
* @param {EachState} state
* @param {Array<V>} array
* @param {Element | Comment | Text} anchor
* @param {number} flags
* @param {(value: V, index: number) => any} get_key
* @returns {void}
*/
function reconcile(state, array, anchor, flags, get_key) {
	var is_animated = (flags & 8) !== 0;
	var length = array.length;
	var items = state.items;
	var current = skip_to_branch(state.effect.first);
	/** @type {undefined | Set<Effect>} */
	var seen;
	/** @type {Effect | null} */
	var prev = null;
	/** @type {undefined | Set<Effect>} */
	var to_animate;
	/** @type {Effect[]} */
	var matched = [];
	/** @type {Effect[]} */
	var stashed = [];
	/** @type {V} */
	var value;
	/** @type {any} */
	var key;
	/** @type {Effect | undefined} */
	var effect;
	/** @type {number} */
	var i;
	if (is_animated) for (i = 0; i < length; i += 1) {
		value = array[i];
		key = get_key(value, i);
		effect = items.get(key).e;
		if ((effect.f & 33554432) === 0) {
			effect.nodes?.a?.measure();
			(to_animate ??= /* @__PURE__ */ new Set()).add(effect);
		}
	}
	for (i = 0; i < length; i += 1) {
		value = array[i];
		key = get_key(value, i);
		effect = items.get(key).e;
		if (state.outrogroups !== null) for (const group of state.outrogroups) {
			group.pending.delete(effect);
			group.done.delete(effect);
		}
		if ((effect.f & 8192) !== 0) {
			resume_effect(effect);
			if (is_animated) {
				effect.nodes?.a?.unfix();
				(to_animate ??= /* @__PURE__ */ new Set()).delete(effect);
			}
		}
		if ((effect.f & 33554432) !== 0) {
			effect.f ^= EFFECT_OFFSCREEN;
			if (effect === current) move(effect, null, anchor);
			else {
				var next = prev ? prev.next : current;
				if (effect === state.effect.last) state.effect.last = effect.prev;
				if (effect.prev) effect.prev.next = effect.next;
				if (effect.next) effect.next.prev = effect.prev;
				link(state, prev, effect);
				link(state, effect, next);
				move(effect, next, anchor);
				prev = effect;
				matched = [];
				stashed = [];
				current = skip_to_branch(prev.next);
				continue;
			}
		}
		if (effect !== current) {
			if (seen !== void 0 && seen.has(effect)) {
				if (matched.length < stashed.length) {
					var start = stashed[0];
					var j;
					prev = start.prev;
					var a = matched[0];
					var b = matched[matched.length - 1];
					for (j = 0; j < matched.length; j += 1) move(matched[j], start, anchor);
					for (j = 0; j < stashed.length; j += 1) seen.delete(stashed[j]);
					link(state, a.prev, b.next);
					link(state, prev, a);
					link(state, b, start);
					current = start;
					prev = b;
					i -= 1;
					matched = [];
					stashed = [];
				} else {
					seen.delete(effect);
					move(effect, current, anchor);
					link(state, effect.prev, effect.next);
					link(state, effect, prev === null ? state.effect.first : prev.next);
					link(state, prev, effect);
					prev = effect;
				}
				continue;
			}
			matched = [];
			stashed = [];
			while (current !== null && current !== effect) {
				(seen ??= /* @__PURE__ */ new Set()).add(current);
				stashed.push(current);
				current = skip_to_branch(current.next);
			}
			if (current === null) continue;
		}
		if ((effect.f & 33554432) === 0) matched.push(effect);
		prev = effect;
		current = skip_to_branch(effect.next);
	}
	if (state.outrogroups !== null) {
		for (const group of state.outrogroups) if (group.pending.size === 0) {
			destroy_effects(state, array_from(group.done));
			state.outrogroups?.delete(group);
		}
		if (state.outrogroups.size === 0) state.outrogroups = null;
	}
	if (current !== null || seen !== void 0) {
		/** @type {Effect[]} */
		var to_destroy = [];
		if (seen !== void 0) {
			for (effect of seen) if ((effect.f & 8192) === 0) to_destroy.push(effect);
		}
		while (current !== null) {
			if ((current.f & 8192) === 0 && current !== state.fallback) to_destroy.push(current);
			current = skip_to_branch(current.next);
		}
		var destroy_length = to_destroy.length;
		if (destroy_length > 0) {
			var controlled_anchor = (flags & 4) !== 0 && length === 0 ? anchor : null;
			if (is_animated) {
				for (i = 0; i < destroy_length; i += 1) to_destroy[i].nodes?.a?.measure();
				for (i = 0; i < destroy_length; i += 1) to_destroy[i].nodes?.a?.fix();
			}
			pause_effects(state, to_destroy, controlled_anchor);
		}
	}
	if (is_animated) queue_micro_task(() => {
		if (to_animate === void 0) return;
		for (effect of to_animate) effect.nodes?.a?.apply();
	});
}
/**
* @template V
* @param {Map<any, EachItem>} items
* @param {Node} anchor
* @param {V} value
* @param {unknown} key
* @param {number} index
* @param {(anchor: Node, item: V | Source<V>, index: number | Value<number>, collection: () => V[]) => void} render_fn
* @param {number} flags
* @param {() => V[]} get_collection
* @returns {EachItem}
*/
function create_item(items, anchor, value, key, index, render_fn, flags, get_collection) {
	var v = (flags & 1) !== 0 ? (flags & 16) === 0 ? /* @__PURE__ */ mutable_source(value, false, false) : source(value) : null;
	var i = (flags & 2) !== 0 ? source(index) : null;
	if (dev_fallback_default && v) v.trace = () => {
		get_collection()[i?.v ?? index];
	};
	return {
		v,
		i,
		e: branch(() => {
			render_fn(anchor, v ?? value, i ?? index, get_collection);
			return () => {
				items.delete(key);
			};
		})
	};
}
/**
* @param {Effect} effect
* @param {Effect | null} next
* @param {Text | Element | Comment} anchor
*/
function move(effect, next, anchor) {
	if (!effect.nodes) return;
	var node = effect.nodes.start;
	var end = effect.nodes.end;
	var dest = next && (next.f & 33554432) === 0 ? next.nodes.start : anchor;
	while (node !== null) {
		var next_node = /* @__PURE__ */ get_next_sibling(node);
		dest.before(node);
		if (node === end) return;
		node = next_node;
	}
}
/**
* @param {EachState} state
* @param {Effect | null} prev
* @param {Effect | null} next
*/
function link(state, prev, next) {
	if (prev === null) state.effect.first = next;
	else prev.next = next;
	if (next === null) state.effect.last = prev;
	else next.prev = prev;
}
/**
* @param {Array<any>} array
* @param {(item: any, index: number) => string} key_fn
* @returns {void}
*/
function validate_each_keys(array, key_fn) {
	const keys = /* @__PURE__ */ new Map();
	const length = array.length;
	for (let i = 0; i < length; i++) {
		const key = key_fn(array[i], i);
		if (keys.has(key)) {
			const a = String(keys.get(key));
			const b = String(i);
			/** @type {string | null} */
			let k = String(key);
			if (k.startsWith("[object ")) k = null;
			each_key_duplicate(a, b, k);
		}
		keys.set(key, i);
	}
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/shared/attributes.js
var whitespace = [..." 	\n\r\f\xA0\v﻿"];
/**
* @param {any} value
* @param {string | null} [hash]
* @param {Record<string, boolean>} [directives]
* @returns {string | null}
*/
function to_class(value, hash, directives) {
	var classname = value == null ? "" : "" + value;
	if (hash) classname = classname ? classname + " " + hash : hash;
	if (directives) {
		for (var key of Object.keys(directives)) if (directives[key]) classname = classname ? classname + " " + key : key;
		else if (classname.length) {
			var len = key.length;
			var a = 0;
			while ((a = classname.indexOf(key, a)) >= 0) {
				var b = a + len;
				if ((a === 0 || whitespace.includes(classname[a - 1])) && (b === classname.length || whitespace.includes(classname[b]))) classname = (a === 0 ? "" : classname.substring(0, a)) + classname.substring(b + 1);
				else a = b;
			}
		}
	}
	return classname === "" ? null : classname;
}
/**
*
* @param {Record<string,any>} styles
* @param {boolean} important
*/
function append_styles(styles, important = false) {
	var separator = important ? " !important;" : ";";
	var css = "";
	for (var key of Object.keys(styles)) {
		var value = styles[key];
		if (value != null && value !== "") css += " " + key + ": " + value + separator;
	}
	return css;
}
/**
* @param {string} name
* @returns {string}
*/
function to_css_name(name) {
	if (name[0] !== "-" || name[1] !== "-") return name.toLowerCase();
	return name;
}
/**
* @param {any} value
* @param {Record<string, any> | [Record<string, any>, Record<string, any>]} [styles]
* @returns {string | null}
*/
function to_style(value, styles) {
	if (styles) {
		var new_style = "";
		/** @type {Record<string,any> | undefined} */
		var normal_styles;
		/** @type {Record<string,any> | undefined} */
		var important_styles;
		if (Array.isArray(styles)) {
			normal_styles = styles[0];
			important_styles = styles[1];
		} else normal_styles = styles;
		if (value) {
			value = String(value).replaceAll(/\s*\/\*.*?\*\/\s*/g, "").trim();
			/** @type {boolean | '"' | "'"} */
			var in_str = false;
			var in_apo = 0;
			var in_comment = false;
			var reserved_names = [];
			if (normal_styles) reserved_names.push(...Object.keys(normal_styles).map(to_css_name));
			if (important_styles) reserved_names.push(...Object.keys(important_styles).map(to_css_name));
			var start_index = 0;
			var name_index = -1;
			const len = value.length;
			for (var i = 0; i < len; i++) {
				var c = value[i];
				if (in_comment) {
					if (c === "/" && value[i - 1] === "*") in_comment = false;
				} else if (in_str) {
					if (in_str === c) in_str = false;
				} else if (c === "/" && value[i + 1] === "*") in_comment = true;
				else if (c === "\"" || c === "'") in_str = c;
				else if (c === "(") in_apo++;
				else if (c === ")") in_apo--;
				if (!in_comment && in_str === false && in_apo === 0) {
					if (c === ":" && name_index === -1) name_index = i;
					else if (c === ";" || i === len - 1) {
						if (name_index !== -1) {
							var name = to_css_name(value.substring(start_index, name_index).trim());
							if (!reserved_names.includes(name)) {
								if (c !== ";") i++;
								var property = value.substring(start_index, i).trim();
								new_style += " " + property + ";";
							}
						}
						start_index = i + 1;
						name_index = -1;
					}
				}
			}
		}
		if (normal_styles) new_style += append_styles(normal_styles);
		if (important_styles) new_style += append_styles(important_styles, true);
		new_style = new_style.trim();
		return new_style === "" ? null : new_style;
	}
	return value == null ? null : String(value);
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/class.js
/**
* @param {Element} dom
* @param {boolean | number} is_html
* @param {string | null} value
* @param {string} [hash]
* @param {Record<string, any>} [prev_classes]
* @param {Record<string, any>} [next_classes]
* @returns {Record<string, boolean> | undefined}
*/
function set_class(dom, is_html, value, hash, prev_classes, next_classes) {
	var prev = dom[CLASS_CACHE];
	if (hydrating || prev !== value || prev === void 0) {
		var next_class_name = to_class(value, hash, next_classes);
		if (!hydrating || next_class_name !== dom.getAttribute("class")) if (next_class_name == null) dom.removeAttribute("class");
		else if (is_html) dom.className = next_class_name;
		else dom.setAttribute("class", next_class_name);
		/** @type {any} */ dom[CLASS_CACHE] = value;
	} else if (next_classes && prev_classes !== next_classes) for (var key in next_classes) {
		var is_present = !!next_classes[key];
		if (prev_classes == null || is_present !== !!prev_classes[key]) dom.classList.toggle(key, is_present);
	}
	return next_classes;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/style.js
/**
* @param {Element & ElementCSSInlineStyle} dom
* @param {Record<string, any>} prev
* @param {Record<string, any>} next
* @param {string} [priority]
*/
function update_styles(dom, prev = {}, next, priority) {
	for (var key in next) {
		var value = next[key];
		if (prev[key] !== value) if (next[key] == null) dom.style.removeProperty(key);
		else dom.style.setProperty(key, value, priority);
	}
}
/**
* @param {Element & ElementCSSInlineStyle} dom
* @param {string | null} value
* @param {Record<string, any> | [Record<string, any>, Record<string, any>]} [prev_styles]
* @param {Record<string, any> | [Record<string, any>, Record<string, any>]} [next_styles]
*/
function set_style(dom, value, prev_styles, next_styles) {
	var prev = dom[STYLE_CACHE];
	if (hydrating || prev !== value) {
		var next_style_attr = to_style(value, next_styles);
		if (!hydrating || next_style_attr !== dom.getAttribute("style")) if (next_style_attr == null) dom.removeAttribute("style");
		else dom.style.cssText = next_style_attr;
		/** @type {any} */ dom[STYLE_CACHE] = value;
	} else if (next_styles) if (Array.isArray(next_styles)) {
		update_styles(dom, prev_styles?.[0], next_styles[0]);
		update_styles(dom, prev_styles?.[1], next_styles[1], "important");
	} else update_styles(dom, prev_styles, next_styles);
	return next_styles;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/bindings/select.js
/**
* Selects the correct option(s) (depending on whether this is a multiple select)
* @template V
* @param {HTMLSelectElement} select
* @param {V} value
* @param {boolean} mounting
*/
function select_option(select, value, mounting = false) {
	if (select.multiple) {
		if (value == void 0) return;
		if (!is_array(value)) return select_multiple_invalid_value();
		for (var option of select.options) option.selected = value.includes(get_option_value(option));
		return;
	}
	for (option of select.options) if (is(get_option_value(option), value)) {
		option.selected = true;
		return;
	}
	if (!mounting || value !== void 0) select.selectedIndex = -1;
}
/**
* Selects the correct option(s) if `value` is given,
* and then sets up a mutation observer to sync the
* current selection to the dom when it changes. Such
* changes could for example occur when options are
* inside an `#each` block.
* @param {HTMLSelectElement} select
*/
function init_select(select) {
	var observer = new MutationObserver(() => {
		select_option(select, select.__value);
	});
	observer.observe(select, {
		childList: true,
		subtree: true,
		attributes: true,
		attributeFilter: ["value"]
	});
	teardown(() => {
		observer.disconnect();
	});
}
/**
* @param {HTMLSelectElement} select
* @param {() => unknown} get
* @param {(value: unknown) => void} set
* @returns {void}
*/
function bind_select_value(select, get, set = get) {
	var batches = /* @__PURE__ */ new WeakSet();
	var mounting = true;
	listen_to_event_and_reset_event(select, "change", (is_reset) => {
		var query = is_reset ? "[selected]" : ":checked";
		/** @type {unknown} */
		var value;
		if (select.multiple) value = [].map.call(select.querySelectorAll(query), get_option_value);
		else {
			/** @type {HTMLOptionElement | null} */
			var selected_option = select.querySelector(query) ?? select.querySelector("option:not([disabled])");
			value = selected_option && get_option_value(selected_option);
		}
		set(value);
		select.__value = value;
		if (current_batch !== null) batches.add(current_batch);
	});
	effect(() => {
		var value = get();
		if (select === document.activeElement) {
			var batch = async_mode_flag ? previous_batch : current_batch;
			if (batches.has(batch)) return;
		}
		select_option(select, value, mounting);
		if (mounting && value === void 0) {
			/** @type {HTMLOptionElement | null} */
			var selected_option = select.querySelector(":checked");
			if (selected_option !== null) {
				value = get_option_value(selected_option);
				set(value);
			}
		}
		select.__value = value;
		mounting = false;
	});
	init_select(select);
}
/** @param {HTMLOptionElement} option */
function get_option_value(option) {
	if ("__value" in option) return option.__value;
	else return option.value;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/attributes.js
/** @import { Blocker, Effect } from '#client' */
var IS_CUSTOM_ELEMENT = Symbol("is custom element");
var IS_HTML = Symbol("is html");
var LINK_TAG = IS_XHTML ? "link" : "LINK";
/**
* The value/checked attribute in the template actually corresponds to the defaultValue property, so we need
* to remove it upon hydration to avoid a bug when someone resets the form value.
* @param {HTMLInputElement} input
* @returns {void}
*/
function remove_input_defaults(input) {
	if (!hydrating) return;
	var already_removed = false;
	var remove_defaults = () => {
		if (already_removed) return;
		already_removed = true;
		if (input.hasAttribute("value")) {
			var value = input.value;
			set_attribute(input, "value", null);
			input.value = value;
		}
		if (input.hasAttribute("checked")) {
			var checked = input.checked;
			set_attribute(input, "checked", null);
			input.checked = checked;
		}
	};
	/** @type {any} */ input[FORM_RESET_HANDLER] = remove_defaults;
	queue_micro_task(remove_defaults);
	add_form_reset_listener();
}
/**
* @param {Element} element
* @param {boolean} checked
*/
function set_checked(element, checked) {
	var attributes = get_attributes(element);
	if (attributes.checked === (attributes.checked = checked ?? void 0)) return;
	element.checked = checked;
}
/**
* @param {Element} element
* @param {string} attribute
* @param {string | null} value
* @param {boolean} [skip_warning]
*/
function set_attribute(element, attribute, value, skip_warning) {
	var attributes = get_attributes(element);
	if (hydrating) {
		attributes[attribute] = element.getAttribute(attribute);
		if (attribute === "src" || attribute === "srcset" || attribute === "href" && element.nodeName === LINK_TAG) {
			if (!skip_warning) check_src_in_dev_hydration(element, attribute, value ?? "");
			return;
		}
	}
	if (attributes[attribute] === (attributes[attribute] = value)) return;
	if (attribute === "loading") element[LOADING_ATTR_SYMBOL] = value;
	if (value == null) element.removeAttribute(attribute);
	else if (typeof value !== "string" && get_setters(element).includes(attribute)) element[attribute] = value;
	else element.setAttribute(attribute, value);
}
/**
*
* @param {Element} element
*/
function get_attributes(element) {
	return element[ATTRIBUTES_CACHE] ??= {
		[IS_CUSTOM_ELEMENT]: element.nodeName.includes("-"),
		[IS_HTML]: element.namespaceURI === NAMESPACE_HTML
	};
}
/** @type {Map<string, string[]>} */
var setters_cache = /* @__PURE__ */ new Map();
/** @param {Element} element */
function get_setters(element) {
	var cache_key = element.getAttribute("is") || element.nodeName;
	var setters = setters_cache.get(cache_key);
	if (setters) return setters;
	setters_cache.set(cache_key, setters = []);
	var descriptors;
	var proto = element;
	var element_proto = Element.prototype;
	while (element_proto !== proto) {
		descriptors = get_descriptors(proto);
		for (var key in descriptors) if (descriptors[key].set && key !== "innerHTML" && key !== "textContent" && key !== "innerText") setters.push(key);
		proto = get_prototype_of(proto);
	}
	return setters;
}
/**
* @param {any} element
* @param {string} attribute
* @param {string} value
*/
function check_src_in_dev_hydration(element, attribute, value) {
	if (!dev_fallback_default) return;
	if (attribute === "srcset" && srcset_url_equal(element, value)) return;
	if (src_url_equal(element.getAttribute(attribute) ?? "", value)) return;
	hydration_attribute_changed(attribute, element.outerHTML.replace(element.innerHTML, element.innerHTML && "..."), String(value));
}
/**
* @param {string} element_src
* @param {string} url
* @returns {boolean}
*/
function src_url_equal(element_src, url) {
	if (element_src === url) return true;
	return new URL(element_src, document.baseURI).href === new URL(url, document.baseURI).href;
}
/** @param {string} srcset */
function split_srcset(srcset) {
	return srcset.split(",").map((src) => src.trim().split(" ").filter(Boolean));
}
/**
* @param {HTMLSourceElement | HTMLImageElement} element
* @param {string} srcset
* @returns {boolean}
*/
function srcset_url_equal(element, srcset) {
	var element_urls = split_srcset(element.srcset);
	var urls = split_srcset(srcset);
	return urls.length === element_urls.length && urls.every(([url, width], i) => width === element_urls[i][1] && (src_url_equal(element_urls[i][0], url) || src_url_equal(url, element_urls[i][0])));
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/dom/elements/bindings/input.js
/** @import { Batch } from '../../../reactivity/batch.js' */
/**
* @param {HTMLInputElement} input
* @param {() => unknown} get
* @param {(value: unknown) => void} set
* @returns {void}
*/
function bind_value(input, get, set = get) {
	var batches = /* @__PURE__ */ new WeakSet();
	listen_to_event_and_reset_event(input, "input", async (is_reset) => {
		if (dev_fallback_default && input.type === "checkbox") bind_invalid_checkbox_value();
		/** @type {any} */
		var value = is_reset ? input.defaultValue : input.value;
		value = is_numberlike_input(input) ? to_number(value) : value;
		set(value);
		if (current_batch !== null) batches.add(current_batch);
		await tick();
		if (value !== (value = get())) {
			var start = input.selectionStart;
			var end = input.selectionEnd;
			var length = input.value.length;
			input.value = value ?? "";
			if (end !== null) {
				var new_length = input.value.length;
				if (start === end && end === length && new_length > length) {
					input.selectionStart = new_length;
					input.selectionEnd = new_length;
				} else {
					input.selectionStart = start;
					input.selectionEnd = Math.min(end, new_length);
				}
			}
		}
	});
	if (hydrating && input.defaultValue !== input.value || untrack(get) == null && input.value) {
		set(is_numberlike_input(input) ? to_number(input.value) : input.value);
		if (current_batch !== null) batches.add(current_batch);
	}
	render_effect(() => {
		if (dev_fallback_default && input.type === "checkbox") bind_invalid_checkbox_value();
		var value = get();
		if (input === document.activeElement) {
			var batch = async_mode_flag ? previous_batch : current_batch;
			if (batches.has(batch)) return;
		}
		if (is_numberlike_input(input) && value === to_number(input.value)) return;
		if (input.type === "date" && !value && !input.value) return;
		if (value !== input.value) input.value = value ?? "";
	});
}
/**
* @param {HTMLInputElement} input
* @param {() => unknown} get
* @param {(value: unknown) => void} set
* @returns {void}
*/
function bind_checked(input, get, set = get) {
	listen_to_event_and_reset_event(input, "change", (is_reset) => {
		set(is_reset ? input.defaultChecked : input.checked);
	});
	if (hydrating && input.defaultChecked !== input.checked || untrack(get) == null) set(input.checked);
	render_effect(() => {
		var value = get();
		input.checked = Boolean(value);
	});
}
/**
* @param {HTMLInputElement} input
*/
function is_numberlike_input(input) {
	var type = input.type;
	return type === "number" || type === "range";
}
/**
* @param {string} value
*/
function to_number(value) {
	return value === "" ? null : +value;
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/client/reactivity/props.js
/** @import { Derived, Effect, Source } from './types.js' */
/**
* This function is responsible for synchronizing a possibly bound prop with the inner component state.
* It is used whenever the compiler sees that the component writes to the prop, or when it has a default prop_value.
* @template V
* @param {Record<string, unknown>} props
* @param {string} key
* @param {number} flags
* @param {V | (() => V)} [fallback]
* @returns {(() => V | ((arg: V) => V) | ((arg: V, mutation: boolean) => V))}
*/
function prop(props, key, flags, fallback) {
	var runes = !legacy_mode_flag || (flags & 2) !== 0;
	var bindable = (flags & 8) !== 0;
	var lazy = (flags & 16) !== 0;
	var fallback_value = fallback;
	var fallback_dirty = true;
	var fallback_signal = void 0;
	var get_fallback = () => {
		if (lazy && runes) {
			fallback_signal ??= /* @__PURE__ */ derived(fallback);
			return get(fallback_signal);
		}
		if (fallback_dirty) {
			fallback_dirty = false;
			fallback_value = lazy ? untrack(fallback) : fallback;
		}
		return fallback_value;
	};
	/** @type {((v: V) => void) | undefined} */
	let setter;
	if (bindable) {
		var is_entry_props = STATE_SYMBOL in props || LEGACY_PROPS in props;
		setter = get_descriptor(props, key)?.set ?? (is_entry_props && key in props ? (v) => props[key] = v : void 0);
	}
	/** @type {V} */
	var initial_value;
	var is_store_sub = false;
	if (bindable) [initial_value, is_store_sub] = capture_store_binding(() => props[key]);
	else initial_value = props[key];
	if (initial_value === void 0 && fallback !== void 0) {
		initial_value = get_fallback();
		if (setter) {
			if (runes) props_invalid_value(key);
			setter(initial_value);
		}
	}
	/** @type {() => V} */
	var getter;
	if (runes) getter = () => {
		var value = props[key];
		if (value === void 0) return get_fallback();
		fallback_dirty = true;
		return value;
	};
	else getter = () => {
		var value = props[key];
		if (value !== void 0) fallback_value = void 0;
		return value === void 0 ? fallback_value : value;
	};
	if (runes && (flags & 4) === 0) return getter;
	if (setter) {
		var legacy_parent = props.$$legacy;
		return (function(value, mutation) {
			if (arguments.length > 0) {
				if (!runes || !mutation || legacy_parent || is_store_sub)
 /** @type {Function} */ setter(mutation ? getter() : value);
				return value;
			}
			return getter();
		});
	}
	var overridden = false;
	var d = ((flags & 1) !== 0 ? derived : derived_safe_equal)(() => {
		overridden = false;
		return getter();
	});
	if (dev_fallback_default) d.label = key;
	if (bindable) get(d);
	var parent_effect = active_effect;
	return (function(value, mutation) {
		if (arguments.length > 0) {
			const new_value = mutation ? get(d) : runes && bindable ? proxy(value) : value;
			set(d, new_value);
			overridden = true;
			if (fallback_value !== void 0) fallback_value = new_value;
			return value;
		}
		if (is_destroying_effect && overridden || (parent_effect.f & 16384) !== 0) return d.v;
		return get(d);
	});
}
if (typeof HTMLElement === "function");
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/index-client.js
/** @import { ComponentContext, ComponentContextLegacy } from '#client' */
/** @import { EventDispatcher } from './index.js' */
/** @import { NotFunction } from './internal/types.js' */
if (dev_fallback_default) {
	/**
	* @param {string} rune
	*/
	function throw_rune_error(rune) {
		if (!(rune in globalThis)) {
			/** @type {any} */
			let value;
			Object.defineProperty(globalThis, rune, {
				configurable: true,
				get: () => {
					if (value !== void 0) return value;
					rune_outside_svelte(rune);
				},
				set: (v) => {
					value = v;
				}
			});
		}
	}
	throw_rune_error("$state");
	throw_rune_error("$effect");
	throw_rune_error("$derived");
	throw_rune_error("$inspect");
	throw_rune_error("$props");
	throw_rune_error("$bindable");
}
/**
* `onMount`, like [`$effect`](https://svelte.dev/docs/svelte/$effect), schedules a function to run as soon as the component has been mounted to the DOM.
* Unlike `$effect`, the provided function only runs once.
*
* It must be called during the component's initialisation (but doesn't need to live _inside_ the component;
* it can be called from an external module). If a function is returned _synchronously_ from `onMount`,
* it will be called when the component is unmounted.
*
* `onMount` functions do not run during [server-side rendering](https://svelte.dev/docs/svelte/svelte-server#render).
*
* @template T
* @param {() => NotFunction<T> | Promise<NotFunction<T>> | (() => any)} fn
* @returns {void}
*/
function onMount(fn) {
	if (component_context === null) lifecycle_outside_component("onMount");
	if (legacy_mode_flag && component_context.l !== null) init_update_callbacks(component_context).m.push(fn);
	else user_effect(() => {
		const cleanup = untrack(fn);
		if (typeof cleanup === "function") return cleanup;
	});
}
/**
* Legacy-mode: Init callbacks object for onMount/beforeUpdate/afterUpdate
* @param {ComponentContext} context
*/
function init_update_callbacks(context) {
	var l = context.l;
	return l.u ??= {
		a: [],
		b: [],
		m: []
	};
}
//#endregion
//#region node_modules/.pnpm/svelte@5.56.3/node_modules/svelte/src/internal/disclose-version.js
if (typeof window !== "undefined") ((window.__svelte ??= {}).v ??= /* @__PURE__ */ new Set()).add("5");
//#endregion
//#region src/lib/format.ts
var numberFormatter = new Intl.NumberFormat("zh-CN");
var rateFormatter = new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 1 });
var timeFormatter = new Intl.DateTimeFormat("zh-CN", {
	hour12: false,
	timeZone: "Asia/Shanghai",
	hour: "2-digit",
	minute: "2-digit",
	second: "2-digit"
});
function formatNumber(value) {
	return value == null || !Number.isFinite(value) ? "--" : numberFormatter.format(value);
}
function formatRate(value) {
	return value == null || !Number.isFinite(value) ? "--" : rateFormatter.format(value);
}
function formatPercent(value) {
	return value == null || !Number.isFinite(value) ? "--" : rateFormatter.format(value);
}
function formatDuration(value) {
	if (value == null || !Number.isFinite(value)) return "--";
	const ms = Math.max(0, value);
	if (ms < 1e3) return `${Math.round(ms)}ms`;
	if (ms < 6e4) return `${(ms / 1e3).toFixed(1)}s`;
	return `${Math.floor(ms / 6e4)}m${Math.floor(ms % 6e4 / 1e3)}s`;
}
function formatTime(unixMillis) {
	if (unixMillis == null || !Number.isFinite(unixMillis)) return "--";
	return timeFormatter.format(new Date(unixMillis));
}
//#endregion
//#region src/lib/integrity-model.ts
var WARMING_STAGES = new Set([
	"connecting",
	"subscribing",
	"backfilling"
]);
function statusLabel(status) {
	return {
		live: "正常",
		closed: "休盘",
		stale: "静默",
		missing: "未收到",
		inactive: "未纳入"
	}[status];
}
function frameIdleMs(metrics, nowMillis) {
	if (metrics.upstream_frame_idle_ms != null) return Number(metrics.upstream_frame_idle_ms);
	if (metrics.last_upstream_frame_unix_secs == null) return null;
	return Math.max(0, nowMillis - metrics.last_upstream_frame_unix_secs * 1e3);
}
function eventIdleMs(metrics, nowMillis) {
	if (metrics.upstream_event_idle_ms != null) return Number(metrics.upstream_event_idle_ms);
	if (metrics.last_decoded_event_unix_secs == null) return null;
	return Math.max(0, nowMillis - metrics.last_decoded_event_unix_secs * 1e3);
}
function flowHealthFor(idleMs, warnAfterMs, criticalAfterMs) {
	if (idleMs == null) return "no_sample";
	if (idleMs > criticalAfterMs) return "critical";
	if (idleMs > warnAfterMs) return "warn";
	return "live";
}
function deriveIntegrity(metrics, snapshot, sampledAt, previous, global = snapshot.summary, globalRowsInput) {
	const rows = Array.isArray(snapshot.symbols) ? snapshot.symbols : [];
	const globalRows = Array.isArray(globalRowsInput) ? globalRowsInput : rows;
	const visibleProblems = rows.filter((row) => row.problem);
	const globalProblems = globalRows.filter((row) => row.problem);
	const observedUniverse = Number(global.universe_observed ?? 0);
	const totalUniverse = Number(global.universe_total || metrics.upstream_symbols || global.total || 0);
	const coverageRatio = totalUniverse > 0 ? observedUniverse / totalUniverse : 0;
	const subscribedProblems = globalProblems.filter((row) => row.subscribed);
	const invalidRowCount = Number(metrics.upstream_invalid_tick_rows || 0);
	const activeInvalidRowCount = Number(global.active_invalid_rows || globalProblems.reduce((sum, row) => sum + Number(row.invalid_rows || 0), 0));
	const gapEventCount = Number(global.gap_event_count || 0);
	const duplicateRowCount = Number(global.duplicate_rows || 0);
	const outOfOrderRowCount = Number(global.out_of_order_rows || 0);
	const confirmedIntegrityIssueCount = 0;
	const diffRowDiscontinuityCount = gapEventCount + duplicateRowCount + outOfOrderRowCount;
	const estimatedMissingRows = Number(global.estimated_missing_rows || 0);
	const upstreamIdleMs = frameIdleMs(metrics, sampledAt);
	const eventIdle = eventIdleMs(metrics, sampledAt);
	const frameFlowHealth = metrics.upstream_frame_idle_health ?? flowHealthFor(upstreamIdleMs, Number(metrics.upstream_frame_idle_warn_after_ms || 2e3), Number(metrics.upstream_frame_idle_critical_after_ms || 5e3));
	const eventFlowHealth = metrics.upstream_event_idle_health ?? flowHealthFor(eventIdle, Number(metrics.upstream_event_idle_warn_after_ms || 3e3), Number(metrics.upstream_event_idle_critical_after_ms || 8e3));
	const decodeHealth = metrics.current_decode_health ?? "healthy";
	const sourceCritical = metrics.upstream_stage === "down" || metrics.upstream_stage === "degraded";
	const idleCritical = frameFlowHealth === "critical" || eventFlowHealth === "critical";
	const idleWarn = frameFlowHealth === "warn" || eventFlowHealth === "warn";
	const decodeWarn = decodeHealth === "degraded";
	const warming = WARMING_STAGES.has(metrics.upstream_stage);
	const elapsedSeconds = previous ? Math.max(.001, (sampledAt - previous.sampledAt) / 1e3) : null;
	const frameRate = elapsedSeconds && previous ? Math.max(0, (metrics.upstream_frames_received - previous.metrics.upstream_frames_received) / elapsedSeconds) : null;
	const eventRate = elapsedSeconds && previous ? Math.max(0, (metrics.upstream_events_decoded - previous.metrics.upstream_events_decoded) / elapsedSeconds) : null;
	const issueCount = Number(global.problem ?? globalProblems.length);
	const subscribedProblemCount = Number(global.subscribed_problem ?? subscribedProblems.length);
	const continuityScore = Math.max(0, 100 - Math.min(55, issueCount * 9) - Math.min(25, (1 - coverageRatio) * 25) - (sourceCritical || idleCritical ? 20 : 0));
	return {
		overall: sourceCritical || idleCritical || subscribedProblemCount > 0 || false ? "critical" : warming && globalRows.length === 0 ? "warming" : idleWarn || decodeWarn || issueCount > 0 || coverageRatio < .98 ? "warning" : "healthy",
		sampledAt,
		metrics,
		snapshot,
		global,
		rows,
		globalRows,
		problems: visibleProblems,
		globalProblems,
		subscribedProblems,
		issueCount,
		subscribedProblemCount,
		invalidRowCount,
		activeInvalidRowCount,
		confirmedIntegrityIssueCount,
		diffRowDiscontinuityCount,
		outOfOrderRowCount,
		estimatedMissingRows,
		upstreamIdleMs,
		eventIdleMs: eventIdle,
		frameFlowHealth,
		eventFlowHealth,
		decodeHealth,
		coverageRatio,
		observedUniverse,
		totalUniverse,
		frameRate,
		eventRate,
		continuityScore
	};
}
//#endregion
//#region src/components/AttentionList.svelte
var root$10 = /* @__PURE__ */ from_html(`<div class="empty svelte-1g474le">当前无活动异常</div>`);
var root_1$7 = /* @__PURE__ */ from_html(`<article><div class="symbol svelte-1g474le"> </div> <div class="desc svelte-1g474le"> </div> <div class="foot svelte-1g474le"> </div></article>`);
var root_2$7 = /* @__PURE__ */ from_html(`<aside class="panel attention svelte-1g474le" data-testid="attention-list"><div class="panel-title">当前关注 · 问题合约</div> <div class="list svelte-1g474le"><!></div></aside>`);
function AttentionList($$anchor, $$props) {
	push($$props, true);
	let ordered = /* @__PURE__ */ user_derived(() => [...$$props.rows].sort((left, right) => {
		return (left.subscribed ? 0 : 10) + (left.problem_severity === "bad" ? 0 : 1) - ((right.subscribed ? 0 : 10) + (right.problem_severity === "bad" ? 0 : 1)) || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1);
	}).slice(0, 24));
	var aside = root_2$7();
	var div = sibling(child(aside), 2);
	var node = child(div);
	var consequent = ($$anchor) => {
		append($$anchor, root$10());
	};
	var alternate = ($$anchor) => {
		var fragment = comment();
		each(first_child(fragment), 17, () => get(ordered), index, ($$anchor, row) => {
			var article = root_1$7();
			var div_2 = child(article);
			var text = child(div_2, true);
			reset(div_2);
			var div_3 = sibling(div_2, 2);
			var text_1 = child(div_3);
			reset(div_3);
			var div_4 = sibling(div_3, 2);
			var text_2 = child(div_4, true);
			reset(div_4);
			reset(article);
			template_effect(($0, $1) => {
				set_class(article, 1, `item ${get(row).problem_severity}`, "svelte-1g474le");
				set_attribute(div_2, "title", get(row).symbol);
				set_text(text, get(row).instrument_name ?? get(row).symbol);
				set_text(text_1, `${$0 ?? ""} · ${$1 ?? ""}`);
				set_text(text_2, get(row).subscribed ? "下游正在使用" : "未订阅");
			}, [() => statusLabel(get(row).status), () => formatDuration(get(row).receive_gap_ms)]);
			append($$anchor, article);
		});
		append($$anchor, fragment);
	};
	if_block(node, ($$render) => {
		if (get(ordered).length === 0) $$render(consequent);
		else $$render(alternate, -1);
	});
	reset(div);
	reset(aside);
	append($$anchor, aside);
	pop();
}
//#endregion
//#region src/lib/timeline.ts
var EXCHANGES = [
	"SHFE",
	"DCE",
	"CZCE",
	"INE",
	"GFEX",
	"CFFEX"
];
function createTimelineHistory() {
	return { samples: [] };
}
function exchangeOf(symbol) {
	return symbol.split(".")[0]?.toUpperCase() || "UNKNOWN";
}
function timelineSeverityForRows(rows) {
	if (rows.length === 0) return "unknown";
	if (rows.every((row) => row.session === "closed")) return "closed";
	if (rows.some((row) => row.problem_severity === "bad")) return "bad";
	if (rows.some((row) => row.problem_severity === "warn")) return "warn";
	if (rows.every((row) => row.flow === "no_sample")) return "no_sample";
	if (rows.every((row) => row.session === "unknown")) return "unknown";
	return "live";
}
function timelineSeverityForRow(row) {
	if (row.session === "closed") return "closed";
	if (row.problem_severity === "bad" || row.integrity === "confirmed_gap") return "bad";
	if (row.problem_severity === "warn" || row.integrity === "suspected" || row.flow === "silent") return "warn";
	if (row.flow === "no_sample") return "no_sample";
	if (row.session === "unknown") return "unknown";
	return "live";
}
function pushTimelineSample(history, model) {
	const rows = model.globalRows;
	const exchangeSeverity = {};
	const symbolSeverity = {};
	for (const exchange of EXCHANGES) exchangeSeverity[exchange] = timelineSeverityForRows(rows.filter((row) => exchangeOf(row.symbol) === exchange));
	for (const row of rows) symbolSeverity[row.symbol] = timelineSeverityForRow(row);
	const subscribedRows = rows.filter((row) => row.subscribed);
	const sample = {
		sampledAt: model.sampledAt,
		exchangeSeverity,
		symbolSeverity,
		subscribedSeverity: timelineSeverityForRows(subscribedRows),
		globalSeverity: model.overall === "critical" ? "bad" : model.overall === "warning" ? "warn" : model.overall === "warming" ? "unknown" : "live"
	};
	history.samples.push(sample);
	history.samples = history.samples.filter((item) => item.sampledAt >= model.sampledAt - 3e5);
	return history;
}
function timelineBuckets(history, now, bucketCount = 60) {
	const bucketMs = 3e5 / bucketCount;
	return Array.from({ length: bucketCount }, (_, index) => {
		const start = now - 3e5 + index * bucketMs;
		const end = start + bucketMs;
		for (let sampleIndex = history.samples.length - 1; sampleIndex >= 0; sampleIndex -= 1) {
			const sample = history.samples[sampleIndex];
			if (sample.sampledAt >= start && sample.sampledAt < end) return sample;
		}
		return null;
	});
}
//#endregion
//#region src/components/ContinuityTimeline.svelte
var root$9 = /* @__PURE__ */ from_html(`<button type="button" class="row-label exchange-row svelte-1vieygf"><span class="caret svelte-1vieygf"> </span> <span class="svelte-1vieygf"> </span> <em class="svelte-1vieygf"> </em></button>`);
var root_1$6 = /* @__PURE__ */ from_html(`<div class="row-label symbol-row svelte-1vieygf"><span class="svelte-1vieygf"> </span> <em class="svelte-1vieygf"> </em></div>`);
var root_2$6 = /* @__PURE__ */ from_html(`<div class="row-label svelte-1vieygf"> </div>`);
var root_3$1 = /* @__PURE__ */ from_html(`<span></span>`);
var root_4$1 = /* @__PURE__ */ from_html(`<!> <!>`, 1);
var root_5$1 = /* @__PURE__ */ from_html(`<section class="panel timeline-panel svelte-1vieygf" data-testid="continuity-timeline"><div class="head svelte-1vieygf"><div class="panel-title">最近 5 分钟连续性</div> <div class="legend svelte-1vieygf"><span class="svelte-1vieygf"><i class="live svelte-1vieygf"></i>正常</span> <span class="svelte-1vieygf"><i class="warn svelte-1vieygf"></i>静默</span> <span class="svelte-1vieygf"><i class="bad svelte-1vieygf"></i>异常</span> <span class="svelte-1vieygf"><i class="closed_unmarked svelte-1vieygf"></i>休盘</span> <span class="svelte-1vieygf"><i class="unknown svelte-1vieygf"></i>未知</span> <span class="svelte-1vieygf"><i class="no_sample svelte-1vieygf"></i>无样本</span></div></div> <div class="timeline svelte-1vieygf"><!> <div class="axis svelte-1vieygf"><span>-5m</span><span>now</span></div></div></section>`);
function ContinuityTimeline($$anchor, $$props) {
	push($$props, true);
	let expandedExchanges = /* @__PURE__ */ state(proxy([]));
	let exchangeRows = /* @__PURE__ */ user_derived(() => EXCHANGES.filter((exchange) => $$props.rows.some((row) => exchangeOf(row.symbol) === exchange)));
	let definitions = /* @__PURE__ */ user_derived(() => [
		{
			kind: "summary",
			key: "global",
			label: "全局",
			severity: (sample) => sample.globalSeverity
		},
		{
			kind: "summary",
			key: "subscribed",
			label: "订阅",
			severity: (sample) => sample.subscribedSeverity
		},
		...get(exchangeRows).flatMap((exchange) => {
			const exchangeSymbols = $$props.rows.filter((row) => exchangeOf(row.symbol) === exchange);
			const issueCount = exchangeSymbols.filter((row) => row.problem_severity === "bad" || row.problem_severity === "warn").length;
			const expanded = get(expandedExchanges).includes(exchange);
			const exchangeDefinition = {
				kind: "exchange",
				key: `exchange:${exchange}`,
				label: exchange,
				exchange,
				issueCount,
				totalCount: exchangeSymbols.length,
				expanded,
				emptySeverity: exchangeSymbols.every((row) => row.session === "closed") ? "closed" : "no_sample",
				severity: (sample) => sample.exchangeSeverity[exchange] ?? "closed"
			};
			if (!expanded) return [exchangeDefinition];
			return [exchangeDefinition, ...orderedSymbolRows(exchangeSymbols).map((row) => ({
				kind: "symbol",
				key: `symbol:${row.symbol}`,
				label: row.instrument_name ?? row.symbol,
				detail: row.subscribed ? "订阅" : "",
				symbol: row.symbol,
				emptySeverity: row.session === "closed" ? "closed" : "no_sample",
				severity: (sample) => sample.symbolSeverity[row.symbol] ?? "closed"
			}))];
		})
	]);
	function cellClass(definition, sample) {
		const severity = sample ? definition.severity(sample) : definition.emptySeverity ?? "no_sample";
		return severity === "closed" ? "closed_unmarked" : severity;
	}
	function orderedSymbolRows(exchangeSymbols) {
		return [...exchangeSymbols].sort((left, right) => severityRank(left) - severityRank(right) || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1)).slice(0, 30);
	}
	function severityRank(row) {
		if (row.problem_severity === "bad") return 0;
		if (row.problem_severity === "warn") return 1;
		if (row.subscribed) return 2;
		if (row.problem_severity === "closed") return 4;
		return 3;
	}
	function toggleExchange(exchange) {
		set(expandedExchanges, get(expandedExchanges).includes(exchange) ? get(expandedExchanges).filter((item) => item !== exchange) : [...get(expandedExchanges), exchange], true);
	}
	var section = root_5$1();
	var div = sibling(child(section), 2);
	each(child(div), 17, () => get(definitions), index, ($$anchor, definition) => {
		var fragment = root_4$1();
		var node_1 = first_child(fragment);
		var consequent = ($$anchor) => {
			var button = root$9();
			var span = child(button);
			var text = child(span, true);
			reset(span);
			var span_1 = sibling(span, 2);
			var text_1 = child(span_1, true);
			reset(span_1);
			var em = sibling(span_1, 2);
			var text_2 = child(em);
			reset(em);
			reset(button);
			template_effect(() => {
				set_attribute(button, "aria-expanded", get(definition).expanded);
				set_attribute(button, "aria-label", `${get(definition).label} ${get(definition).issueCount}/${get(definition).totalCount} 异常`);
				set_text(text, get(definition).expanded ? "−" : "+");
				set_text(text_1, get(definition).label);
				set_text(text_2, `${get(definition).issueCount ?? ""}/${get(definition).totalCount ?? ""}`);
			});
			delegated("click", button, () => toggleExchange(get(definition).exchange));
			append($$anchor, button);
		};
		var consequent_1 = ($$anchor) => {
			var div_1 = root_1$6();
			var span_2 = child(div_1);
			var text_3 = child(span_2, true);
			reset(span_2);
			var em_1 = sibling(span_2, 2);
			var text_4 = child(em_1, true);
			reset(em_1);
			reset(div_1);
			template_effect(() => {
				set_attribute(div_1, "title", get(definition).symbol);
				set_text(text_3, get(definition).label);
				set_text(text_4, get(definition).detail);
			});
			append($$anchor, div_1);
		};
		var alternate = ($$anchor) => {
			var div_2 = root_2$6();
			var text_5 = child(div_2, true);
			reset(div_2);
			template_effect(() => set_text(text_5, get(definition).label));
			append($$anchor, div_2);
		};
		if_block(node_1, ($$render) => {
			if (get(definition).kind === "exchange") $$render(consequent);
			else if (get(definition).kind === "symbol") $$render(consequent_1, 1);
			else $$render(alternate, -1);
		});
		each(sibling(node_1, 2), 17, () => $$props.buckets, index, ($$anchor, bucket) => {
			var span_3 = root_3$1();
			template_effect(($0) => set_class(span_3, 1, $0, "svelte-1vieygf"), [() => `cell ${cellClass(get(definition), get(bucket))}`]);
			append($$anchor, span_3);
		});
		append($$anchor, fragment);
	});
	next(2);
	reset(div);
	reset(section);
	template_effect(() => set_style(div, `--bucket-count:${$$props.buckets.length}`));
	append($$anchor, section);
	pop();
}
delegate(["click"]);
//#endregion
//#region src/components/DashboardControls.svelte
var root$8 = /* @__PURE__ */ from_html(`<label class="svelte-jyijee"><input type="checkbox" class="svelte-jyijee"/> <span> </span></label>`);
var root_1$5 = /* @__PURE__ */ from_html(`<option> </option>`);
var root_2$5 = /* @__PURE__ */ from_html(`<details class="panel controls svelte-jyijee" data-testid="dashboard-controls"><summary class="svelte-jyijee">筛选</summary> <div class="control-grid svelte-jyijee"><div class="status-set svelte-jyijee" aria-label="status filters"></div> <label class="toggle svelte-jyijee"><input type="checkbox" class="svelte-jyijee"/> <span>只看订阅</span></label> <input class="search svelte-jyijee" placeholder="搜索合约或中文名"/> <select class="svelte-jyijee"></select> <select class="svelte-jyijee"></select> <button type="button" class="svelte-jyijee">刷新</button></div></details>`);
function DashboardControls($$anchor, $$props) {
	push($$props, true);
	const statuses = [
		{
			value: "live",
			label: "正常"
		},
		{
			value: "closed",
			label: "休盘"
		},
		{
			value: "stale",
			label: "静默"
		},
		{
			value: "missing",
			label: "未收到"
		},
		{
			value: "inactive",
			label: "未纳入"
		}
	];
	const sorts = [
		{
			value: "receive_gap_ms_desc",
			label: "接收延迟"
		},
		{
			value: "market_time_lag_ms_desc",
			label: "行情时间延迟"
		},
		{
			value: "ticks_ingested_desc",
			label: "Tick 累计"
		},
		{
			value: "status_asc",
			label: "状态"
		},
		{
			value: "symbol_asc",
			label: "合约"
		}
	];
	let filters = prop($$props, "filters", 15);
	let search = /* @__PURE__ */ state(proxy(filters().q));
	function toggleStatus(status, checked) {
		filters(filters().statuses = checked ? [...new Set([...filters().statuses, status])] : filters().statuses.filter((item) => item !== status), true);
	}
	function checkboxValue(event) {
		return event.currentTarget.checked;
	}
	function refresh() {
		filters(filters().q = get(search), true);
		return $$props.onrefresh();
	}
	user_effect(() => {
		const value = get(search);
		const timer = window.setTimeout(() => {
			if (filters().q !== value) filters(filters().q = value, true);
		}, 300);
		return () => window.clearTimeout(timer);
	});
	var details = root_2$5();
	var div = sibling(child(details), 2);
	var div_1 = child(div);
	each(div_1, 21, () => statuses, index, ($$anchor, status) => {
		var label = root$8();
		var input = child(label);
		remove_input_defaults(input);
		var span = sibling(input, 2);
		var text = child(span, true);
		reset(span);
		reset(label);
		template_effect(($0) => {
			set_checked(input, $0);
			input.disabled = $$props.disabled;
			set_text(text, get(status).label);
		}, [() => filters().statuses.includes(get(status).value)]);
		delegated("change", input, (event) => toggleStatus(get(status).value, checkboxValue(event)));
		append($$anchor, label);
	});
	reset(div_1);
	var label_1 = sibling(div_1, 2);
	var input_1 = child(label_1);
	remove_input_defaults(input_1);
	next(2);
	reset(label_1);
	var input_2 = sibling(label_1, 2);
	remove_input_defaults(input_2);
	var select = sibling(input_2, 2);
	each(select, 21, () => sorts, index, ($$anchor, sort) => {
		var option = root_1$5();
		var text_1 = child(option, true);
		reset(option);
		var option_value = {};
		template_effect(() => {
			set_text(text_1, get(sort).label);
			if (option_value !== (option_value = get(sort).value)) option.value = (option.__value = get(sort).value) ?? "";
		});
		append($$anchor, option);
	});
	reset(select);
	var select_1 = sibling(select, 2);
	each(select_1, 20, () => [
		50,
		100,
		200,
		500
	], index, ($$anchor, limit) => {
		var option_1 = root_1$5();
		var text_2 = child(option_1, true);
		reset(option_1);
		var option_1_value = {};
		template_effect(() => {
			set_text(text_2, limit);
			if (option_1_value !== (option_1_value = limit)) option_1.value = (option_1.__value = limit) ?? "";
		});
		append($$anchor, option_1);
	});
	reset(select_1);
	var button = sibling(select_1, 2);
	reset(div);
	reset(details);
	template_effect(() => {
		input_1.disabled = $$props.disabled;
		input_2.disabled = $$props.disabled;
		select.disabled = $$props.disabled;
		select_1.disabled = $$props.disabled;
		button.disabled = $$props.disabled;
	});
	bind_checked(input_1, () => filters().subscribedOnly, ($$value) => filters(filters().subscribedOnly = $$value, true));
	bind_value(input_2, () => get(search), ($$value) => set(search, $$value));
	bind_select_value(select, () => filters().sort, ($$value) => filters(filters().sort = $$value, true));
	bind_select_value(select_1, () => filters().limit, ($$value) => filters(filters().limit = $$value, true));
	delegated("click", button, refresh);
	append($$anchor, details);
	pop();
}
delegate(["change", "click"]);
//#endregion
//#region src/components/IncidentTable.svelte
var root$7 = /* @__PURE__ */ from_html(`<tr><td colspan="5" class="empty-cell svelte-1qf9j4q">本页尚未观测到状态变化</td></tr>`);
var root_1$4 = /* @__PURE__ */ from_html(`<tr><td> </td><td> </td><td><span> </span></td><td> </td><td> </td></tr>`);
var root_2$4 = /* @__PURE__ */ from_html(`<section class="panel incidents svelte-1qf9j4q" data-testid="incident-table"><div class="panel-title">断流 / 覆盖事件</div> <table class="table"><thead><tr><th class="svelte-1qf9j4q">时间</th><th class="svelte-1qf9j4q">范围</th><th class="svelte-1qf9j4q">类型</th><th class="svelte-1qf9j4q">详情</th><th class="svelte-1qf9j4q">影响</th></tr></thead><tbody><!></tbody></table></section>`);
function IncidentTable($$anchor, $$props) {
	push($$props, true);
	var section = root_2$4();
	var table = sibling(child(section), 2);
	var tbody = sibling(child(table));
	var node = child(tbody);
	var consequent = ($$anchor) => {
		append($$anchor, root$7());
	};
	var alternate = ($$anchor) => {
		var fragment = comment();
		each(first_child(fragment), 17, () => $$props.incidents.slice(0, 8), index, ($$anchor, incident) => {
			var tr_1 = root_1$4();
			var td = child(tr_1);
			var text = child(td, true);
			reset(td);
			var td_1 = sibling(td);
			var text_1 = child(td_1, true);
			reset(td_1);
			var td_2 = sibling(td_1);
			var span = child(td_2);
			var text_2 = child(span, true);
			reset(span);
			reset(td_2);
			var td_3 = sibling(td_2);
			var text_3 = child(td_3, true);
			reset(td_3);
			var td_4 = sibling(td_3);
			var text_4 = child(td_4, true);
			reset(td_4);
			reset(tr_1);
			template_effect(($0) => {
				set_text(text, $0);
				set_attribute(td_1, "title", get(incident).scope_symbol);
				set_text(text_1, get(incident).scope);
				set_class(span, 1, `badge ${get(incident).severity}`, "svelte-1qf9j4q");
				set_text(text_2, get(incident).type);
				set_attribute(td_3, "title", get(incident).detail);
				set_text(text_3, get(incident).detail);
				set_text(text_4, get(incident).impact);
			}, [() => formatTime(get(incident).at)]);
			append($$anchor, tr_1);
		});
		append($$anchor, fragment);
	};
	if_block(node, ($$render) => {
		if ($$props.incidents.length === 0) $$render(consequent);
		else $$render(alternate, -1);
	});
	reset(tbody);
	reset(table);
	reset(section);
	append($$anchor, section);
	pop();
}
//#endregion
//#region src/components/ScoreGauge.svelte
var root$6 = /* @__PURE__ */ from_html(`<div data-testid="score-gauge"><div class="inner svelte-1n5qzgf"><span class="svelte-1n5qzgf">连续性评分</span> <b class="svelte-1n5qzgf"> </b> <em class="svelte-1n5qzgf"> </em></div></div>`);
function ScoreGauge($$anchor, $$props) {
	push($$props, true);
	let compact = prop($$props, "compact", 3, false);
	let clamped = /* @__PURE__ */ user_derived(() => Math.max(0, Math.min(100, $$props.score)));
	let tone = /* @__PURE__ */ user_derived(() => get(clamped) < 60 ? "bad" : get(clamped) < 85 ? "warn" : "live");
	var div = root$6();
	var div_1 = child(div);
	var b = sibling(child(div_1), 2);
	var text = child(b, true);
	reset(b);
	var em = sibling(b, 2);
	var text_1 = child(em, true);
	reset(em);
	reset(div_1);
	reset(div);
	template_effect(($0) => {
		set_class(div, 1, `gauge ${get(tone)} ${compact() ? "compact" : ""}`, "svelte-1n5qzgf");
		set_style(div, `--angle:${get(clamped) * 3.6}deg`);
		set_text(text, $0);
		set_text(text_1, get(tone) === "bad" ? "告警" : get(tone) === "warn" ? "关注" : "连续");
	}, [() => formatNumber(Math.round(get(clamped)))]);
	append($$anchor, div);
	pop();
}
//#endregion
//#region src/components/IntegrityHero.svelte
var root$5 = /* @__PURE__ */ from_html(`<section data-testid="integrity-hero"><div class="orb svelte-4y4rc7"><span class="shield svelte-4y4rc7"> </span></div> <div class="copy"><h2 class="svelte-4y4rc7"> </h2> <p class="svelte-4y4rc7"><b class="svelte-4y4rc7"> </b> </p></div> <div class="ecg svelte-4y4rc7" aria-hidden="true"><svg viewBox="0 0 190 58" class="svelte-4y4rc7"><polyline points="0,31 72,31 82,24 90,38 98,8 106,50 115,20 124,31 190,31" class="svelte-4y4rc7"></polyline></svg></div> <div class="score svelte-4y4rc7"><!></div></section>`);
function IntegrityHero($$anchor, $$props) {
	push($$props, true);
	function heroTitle(model) {
		if (model.overall === "critical") return "订阅链路告警";
		if (model.overall === "warning") return "行情静默预警";
		if (model.overall === "warming") return "启动观测中";
		return "行情链路连续";
	}
	let title = /* @__PURE__ */ user_derived(() => heroTitle($$props.model));
	let tone = /* @__PURE__ */ user_derived(() => $$props.model.overall === "critical" ? "error" : $$props.model.overall === "warning" ? "warning" : $$props.model.overall === "warming" ? "standby" : "live");
	let icon = /* @__PURE__ */ user_derived(() => $$props.model.overall === "critical" ? "!" : $$props.model.overall === "warning" ? "!" : $$props.model.overall === "warming" ? "…" : "✓");
	let subtitle = /* @__PURE__ */ user_derived(() => `${formatNumber($$props.model.observedUniverse)}/${formatNumber($$props.model.totalUniverse)} 合约有接收记录，覆盖 ${formatPercent($$props.model.coverageRatio * 100)}%，Diff行号诊断 ${formatNumber($$props.model.diffRowDiscontinuityCount)} 次，跳号估算 ${formatNumber($$props.model.estimatedMissingRows)} 行，倒序 ${formatNumber($$props.model.outOfOrderRowCount)} 行，帧静默 ${formatDuration($$props.model.upstreamIdleMs)}，事件静默 ${formatDuration($$props.model.eventIdleMs)}`);
	var section = root$5();
	var div = child(section);
	var span = child(div);
	var text = child(span, true);
	reset(span);
	reset(div);
	var div_1 = sibling(div, 2);
	var h2 = child(div_1);
	var text_1 = child(h2, true);
	reset(h2);
	var p = sibling(h2, 2);
	var b = child(p);
	var text_2 = child(b, true);
	reset(b);
	var text_3 = sibling(b);
	reset(p);
	reset(div_1);
	var div_2 = sibling(div_1, 4);
	ScoreGauge(child(div_2), {
		get score() {
			return $$props.model.continuityScore;
		},
		compact: true
	});
	reset(div_2);
	reset(section);
	template_effect(($0) => {
		set_class(section, 1, `panel hero ${get(tone)}`, "svelte-4y4rc7");
		set_text(text, get(icon));
		set_text(text_1, get(title));
		set_text(text_2, $0);
		set_text(text_3, ` 个异常 · ${get(subtitle) ?? ""}`);
	}, [() => formatNumber($$props.model.issueCount)]);
	append($$anchor, section);
	pop();
}
//#endregion
//#region src/components/MetricCard.svelte
var root$4 = /* @__PURE__ */ from_html(`<article><div class="icon svelte-1iu5zja"> </div> <div class="body"><div class="label svelte-1iu5zja"> </div> <div class="value svelte-1iu5zja"> <span class="svelte-1iu5zja"> </span></div></div></article>`);
function MetricCard($$anchor, $$props) {
	push($$props, true);
	let unit = prop($$props, "unit", 3, ""), tone = prop($$props, "tone", 3, "info"), format = prop($$props, "format", 3, "number");
	let display = /* @__PURE__ */ user_derived(() => format() === "duration" ? formatDuration($$props.value) : format() === "rate" ? formatRate($$props.value) : format() === "percent" ? formatPercent($$props.value) : formatNumber($$props.value));
	var article = root$4();
	var div = child(article);
	var text = child(div, true);
	reset(div);
	var div_1 = sibling(div, 2);
	var div_2 = child(div_1);
	var text_1 = child(div_2, true);
	reset(div_2);
	var div_3 = sibling(div_2, 2);
	var text_2 = child(div_3, true);
	var span = sibling(text_2);
	var text_3 = child(span, true);
	reset(span);
	reset(div_3);
	reset(div_1);
	reset(article);
	template_effect(() => {
		set_class(article, 1, `panel metric ${tone()}`, "svelte-1iu5zja");
		set_text(text, $$props.icon);
		set_text(text_1, $$props.label);
		set_text(text_2, get(display));
		set_text(text_3, unit());
	});
	append($$anchor, article);
	pop();
}
//#endregion
//#region src/components/MonitorHeader.svelte
var root$3 = /* @__PURE__ */ from_html(`<span class="muted svelte-f1m687"> </span>`);
var root_1$3 = /* @__PURE__ */ from_html(`<div class="panel error-banner svelte-f1m687"> </div>`);
var root_2$3 = /* @__PURE__ */ from_html(`<header class="header svelte-f1m687" data-testid="monitor-header"><div class="left svelte-f1m687"><span class="env svelte-f1m687">RELAY</span> <span>◉ Asia/Shanghai</span> <span> </span></div> <h1 class="brand svelte-f1m687">tqsdk-relay 行情完整性监控中心</h1> <div class="right svelte-f1m687"><!> <span><span></span> <span> </span></span> <button type="button" class="svelte-f1m687"> </button> <button type="button" class="svelte-f1m687"> </button></div></header> <!>`, 1);
function MonitorHeader($$anchor, $$props) {
	push($$props, true);
	let paused = prop($$props, "paused", 15, false), fullscreen = prop($$props, "fullscreen", 15, false);
	let now = /* @__PURE__ */ state(proxy(Date.now()));
	let fullscreenSupported = /* @__PURE__ */ state(true);
	let stateLabel = /* @__PURE__ */ user_derived(() => paused() ? "已暂停" : $$props.error ? "读取异常" : "实时监控中");
	let stateClass = /* @__PURE__ */ user_derived(() => $$props.error ? "bad" : paused() ? "closed" : "live");
	onMount(() => {
		set(fullscreenSupported, typeof document.documentElement.requestFullscreen === "function");
		const syncFullscreen = () => {
			fullscreen(document.fullscreenElement != null);
		};
		document.addEventListener("fullscreenchange", syncFullscreen);
		syncFullscreen();
		return () => document.removeEventListener("fullscreenchange", syncFullscreen);
	});
	user_effect(() => {
		const timer = window.setInterval(() => {
			set(now, Date.now(), true);
		}, 1e3);
		return () => window.clearInterval(timer);
	});
	async function toggleFullscreen() {
		if (!get(fullscreenSupported)) return;
		try {
			if (document.fullscreenElement) await document.exitFullscreen();
			else await document.documentElement.requestFullscreen();
			fullscreen(document.fullscreenElement != null);
		} catch {
			fullscreen(document.fullscreenElement != null);
		}
	}
	var fragment = root_2$3();
	var header = first_child(fragment);
	var div = child(header);
	var span = sibling(child(div), 4);
	var text = child(span, true);
	reset(span);
	reset(div);
	var div_1 = sibling(div, 4);
	var node = child(div_1);
	var consequent = ($$anchor) => {
		var span_1 = root$3();
		var text_1 = child(span_1);
		reset(span_1);
		template_effect(($0) => set_text(text_1, `采样 ${$0 ?? ""}`), [() => formatTime($$props.model.sampledAt)]);
		append($$anchor, span_1);
	};
	if_block(node, ($$render) => {
		if ($$props.model) $$render(consequent);
	});
	var span_2 = sibling(node, 2);
	var span_3 = child(span_2);
	var span_4 = sibling(span_3, 2);
	var text_2 = child(span_4, true);
	reset(span_4);
	reset(span_2);
	var button = sibling(span_2, 2);
	var text_3 = child(button, true);
	reset(button);
	var button_1 = sibling(button, 2);
	var text_4 = child(button_1, true);
	reset(button_1);
	reset(div_1);
	reset(header);
	var node_1 = sibling(header, 2);
	var consequent_1 = ($$anchor) => {
		var div_2 = root_1$3();
		var text_5 = child(div_2, true);
		reset(div_2);
		template_effect(() => set_text(text_5, $$props.error));
		append($$anchor, div_2);
	};
	if_block(node_1, ($$render) => {
		if ($$props.error) $$render(consequent_1);
	});
	template_effect(($0) => {
		set_text(text, $0);
		set_class(span_2, 1, `live-chip ${get(stateClass) === "bad" ? "offline" : ""}`, "svelte-f1m687");
		set_class(span_3, 1, `status-dot ${get(stateClass)}`, "svelte-f1m687");
		set_text(text_2, get(stateLabel));
		set_text(text_3, paused() ? "继续" : "暂停");
		button_1.disabled = !get(fullscreenSupported);
		set_attribute(button_1, "title", get(fullscreenSupported) ? "切换全屏" : "当前浏览器不支持全屏");
		set_text(text_4, fullscreen() ? "退出" : "全屏");
	}, [() => formatTime(get(now))]);
	delegated("click", button, () => paused(!paused()));
	delegated("click", button_1, toggleFullscreen);
	append($$anchor, fragment);
	pop();
}
delegate(["click"]);
//#endregion
//#region src/components/RelayPipeline.svelte
var root$2 = /* @__PURE__ */ from_html(`<div class="arrow svelte-dg2yd7"></div>`);
var root_1$2 = /* @__PURE__ */ from_html(`<div class="node svelte-dg2yd7"><div class="node-icon svelte-dg2yd7"> </div> <div class="node-copy svelte-dg2yd7"><div class="name svelte-dg2yd7"> </div> <div> </div> <div class="meta svelte-dg2yd7"> </div></div> <span></span></div> <!>`, 1);
var root_2$2 = /* @__PURE__ */ from_html(`<section class="panel pipeline svelte-dg2yd7" data-testid="relay-pipeline"></section>`);
function RelayPipeline($$anchor, $$props) {
	push($$props, true);
	function cacheState(model) {
		if (model.frameFlowHealth === "critical") return "帧流中断";
		if (model.issueCount > 0 || model.frameFlowHealth === "warn") return "需关注";
		return "活跃";
	}
	function cacheMeta(model) {
		return `帧 ${formatDuration(model.upstreamIdleMs)} / 事件 ${formatDuration(model.eventIdleMs)}`;
	}
	function cacheSeverity(model) {
		if (model.frameFlowHealth === "critical") return "bad";
		if (model.issueCount > 0 || model.frameFlowHealth === "warn" || model.eventFlowHealth === "warn") return "warn";
		return "live";
	}
	let nodes = /* @__PURE__ */ user_derived(() => [
		{
			name: "上游连接",
			icon: "☁",
			state: $$props.model.metrics.upstream_stage,
			meta: `frame ${formatNumber($$props.model.metrics.upstream_frames_received)}`,
			severity: $$props.model.metrics.upstream_stage === "live" ? "live" : $$props.model.metrics.upstream_stage === "down" || $$props.model.metrics.upstream_stage === "degraded" ? "bad" : "warn"
		},
		{
			name: "合约集合",
			icon: "▦",
			state: `${formatNumber($$props.model.totalUniverse)} 合约`,
			meta: `覆盖 ${formatNumber($$props.model.observedUniverse)}`,
			severity: $$props.model.coverageRatio >= .98 ? "live" : $$props.model.coverageRatio >= .9 ? "warn" : "bad"
		},
		{
			name: "数据解码",
			icon: "⌘",
			state: $$props.model.decodeHealth === "degraded" ? "解析诊断" : "正常",
			meta: `${formatNumber($$props.model.metrics.recent_invalid_rows_1m)} 近期 / ${formatNumber($$props.model.invalidRowCount)} 累计`,
			severity: $$props.model.decodeHealth === "degraded" ? "warn" : "live"
		},
		{
			name: "行情缓存",
			icon: "◫",
			state: cacheState($$props.model),
			meta: cacheMeta($$props.model),
			severity: cacheSeverity($$props.model)
		},
		{
			name: "下游服务",
			icon: "▤",
			state: $$props.model.subscribedProblemCount > 0 ? "影响订阅" : "正常",
			meta: `${formatNumber($$props.model.metrics.downstream_clients)} 客户端`,
			severity: $$props.model.subscribedProblemCount > 0 ? "bad" : "live"
		}
	]);
	var section = root_2$2();
	each(section, 21, () => get(nodes), index, ($$anchor, node, index) => {
		var fragment = root_1$2();
		var div = first_child(fragment);
		var div_1 = child(div);
		var text = child(div_1, true);
		reset(div_1);
		var div_2 = sibling(div_1, 2);
		var div_3 = child(div_2);
		var text_1 = child(div_3, true);
		reset(div_3);
		var div_4 = sibling(div_3, 2);
		var text_2 = child(div_4, true);
		reset(div_4);
		var div_5 = sibling(div_4, 2);
		var text_3 = child(div_5, true);
		reset(div_5);
		reset(div_2);
		var span = sibling(div_2, 2);
		reset(div);
		var node_1 = sibling(div, 2);
		var consequent = ($$anchor) => {
			append($$anchor, root$2());
		};
		if_block(node_1, ($$render) => {
			if (index < get(nodes).length - 1) $$render(consequent);
		});
		template_effect(() => {
			set_text(text, get(node).icon);
			set_text(text_1, get(node).name);
			set_class(div_4, 1, `state ${get(node).severity}`, "svelte-dg2yd7");
			set_text(text_2, get(node).state);
			set_text(text_3, get(node).meta);
			set_class(span, 1, `status-dot ${get(node).severity}`, "svelte-dg2yd7");
		});
		append($$anchor, fragment);
	});
	reset(section);
	append($$anchor, section);
	pop();
}
//#endregion
//#region src/components/SymbolHealthTable.svelte
var root$1 = /* @__PURE__ */ from_html(`<div class="selected svelte-1bcfi66"> </div>`);
var root_1$1 = /* @__PURE__ */ from_html(`<tr class="svelte-1bcfi66"><td colspan="7" class="empty-cell svelte-1bcfi66">等待合约数据</td></tr>`);
var root_2$1 = /* @__PURE__ */ from_html(`<tr><td class="svelte-1bcfi66"><span> </span></td><td class="svelte-1bcfi66"> </td><td class="svelte-1bcfi66"> </td><td class="svelte-1bcfi66"> </td><td class="svelte-1bcfi66"> </td><td class="svelte-1bcfi66"> </td><td class="svelte-1bcfi66"><span><i class="svelte-1bcfi66"></i> </span></td></tr>`);
var root_3 = /* @__PURE__ */ from_html(`<span class="error svelte-1bcfi66"> </span>`);
var root_4 = /* @__PURE__ */ from_html(`<div class="detail svelte-1bcfi66"><span class="svelte-1bcfi66">last price <b class="svelte-1bcfi66"> </b></span> <span class="svelte-1bcfi66">volume <b class="svelte-1bcfi66"> </b></span> <span class="svelte-1bcfi66">open interest <b class="svelte-1bcfi66"> </b></span> <span class="svelte-1bcfi66">invalid rows <b class="svelte-1bcfi66"> </b></span> <!></div>`);
var root_5 = /* @__PURE__ */ from_html(`<section class="panel table-panel svelte-1bcfi66" data-testid="symbol-health-table"><div class="head svelte-1bcfi66"><div class="panel-title">活跃合约健康排行</div> <div class="count svelte-1bcfi66"> </div> <!></div> <table class="table"><thead><tr class="svelte-1bcfi66"><th class="svelte-1bcfi66">状态</th><th class="svelte-1bcfi66">名称</th><th class="svelte-1bcfi66">距上次更新</th><th class="svelte-1bcfi66">行情延迟</th><th class="svelte-1bcfi66">Tick</th><th class="svelte-1bcfi66">订阅</th><th class="svelte-1bcfi66">风险</th></tr></thead><tbody><!></tbody></table> <!></section>`);
function SymbolHealthTable($$anchor, $$props) {
	push($$props, true);
	let selectedSymbol = prop($$props, "selectedSymbol", 15, null);
	let ordered = /* @__PURE__ */ user_derived(() => [...$$props.rows].sort((left, right) => {
		return severityRank(left) - severityRank(right) || (right.receive_gap_ms ?? -1) - (left.receive_gap_ms ?? -1);
	}));
	let selected = /* @__PURE__ */ user_derived(() => $$props.rows.find((row) => row.symbol === selectedSymbol()) ?? get(ordered)[0] ?? null);
	function severityRank(row) {
		if (row.problem_severity === "bad") return 0;
		if (row.problem_severity === "warn") return 1;
		if (row.subscribed) return 2;
		if (row.problem_severity === "closed") return 4;
		return 3;
	}
	var section = root_5();
	var div = child(section);
	var div_1 = sibling(child(div), 2);
	var text = child(div_1);
	reset(div_1);
	var node = sibling(div_1, 2);
	var consequent = ($$anchor) => {
		var div_2 = root$1();
		var text_1 = child(div_2, true);
		reset(div_2);
		template_effect(() => {
			set_attribute(div_2, "title", get(selected).symbol);
			set_text(text_1, get(selected).instrument_name ?? get(selected).symbol);
		});
		append($$anchor, div_2);
	};
	if_block(node, ($$render) => {
		if (get(selected)) $$render(consequent);
	});
	reset(div);
	var table = sibling(div, 2);
	var tbody = sibling(child(table));
	var node_1 = child(tbody);
	var consequent_1 = ($$anchor) => {
		append($$anchor, root_1$1());
	};
	var alternate = ($$anchor) => {
		var fragment = comment();
		each(first_child(fragment), 17, () => get(ordered), index, ($$anchor, row) => {
			var tr_1 = root_2$1();
			let classes;
			var td = child(tr_1);
			var span = child(td);
			var text_2 = child(span, true);
			reset(span);
			reset(td);
			var td_1 = sibling(td);
			var text_3 = child(td_1, true);
			reset(td_1);
			var td_2 = sibling(td_1);
			var text_4 = child(td_2, true);
			reset(td_2);
			var td_3 = sibling(td_2);
			var text_5 = child(td_3, true);
			reset(td_3);
			var td_4 = sibling(td_3);
			var text_6 = child(td_4, true);
			reset(td_4);
			var td_5 = sibling(td_4);
			var text_7 = child(td_5, true);
			reset(td_5);
			var td_6 = sibling(td_5);
			var span_1 = child(td_6);
			var text_8 = sibling(child(span_1), 1, true);
			reset(span_1);
			reset(td_6);
			reset(tr_1);
			template_effect(($0, $1, $2, $3, $4) => {
				classes = set_class(tr_1, 1, "svelte-1bcfi66", null, classes, { selected: get(row).symbol === selectedSymbol() });
				set_class(span, 1, `badge ${get(row).status}`, "svelte-1bcfi66");
				set_text(text_2, $0);
				set_attribute(td_1, "title", get(row).symbol);
				set_text(text_3, get(row).instrument_name ?? get(row).symbol);
				set_text(text_4, $1);
				set_text(text_5, $2);
				set_text(text_6, $3);
				set_text(text_7, $4);
				set_class(span_1, 1, `risk ${get(row).problem_severity}`, "svelte-1bcfi66");
				set_text(text_8, get(row).problem_severity);
			}, [
				() => statusLabel(get(row).status),
				() => formatDuration(get(row).receive_gap_ms),
				() => formatDuration(get(row).market_time_lag_ms),
				() => formatNumber(get(row).ticks_ingested),
				() => formatNumber(get(row).quote_subscriber_count + get(row).chart_subscriber_count)
			]);
			delegated("click", tr_1, () => selectedSymbol(get(row).symbol));
			append($$anchor, tr_1);
		});
		append($$anchor, fragment);
	};
	if_block(node_1, ($$render) => {
		if (get(ordered).length === 0) $$render(consequent_1);
		else $$render(alternate, -1);
	});
	reset(tbody);
	reset(table);
	var node_3 = sibling(table, 2);
	var consequent_3 = ($$anchor) => {
		var div_3 = root_4();
		var span_2 = child(div_3);
		var b = sibling(child(span_2));
		var text_9 = child(b, true);
		reset(b);
		reset(span_2);
		var span_3 = sibling(span_2, 2);
		var b_1 = sibling(child(span_3));
		var text_10 = child(b_1, true);
		reset(b_1);
		reset(span_3);
		var span_4 = sibling(span_3, 2);
		var b_2 = sibling(child(span_4));
		var text_11 = child(b_2, true);
		reset(b_2);
		reset(span_4);
		var span_5 = sibling(span_4, 2);
		var b_3 = sibling(child(span_5));
		var text_12 = child(b_3, true);
		reset(b_3);
		reset(span_5);
		var node_4 = sibling(span_5, 2);
		var consequent_2 = ($$anchor) => {
			var span_6 = root_3();
			var text_13 = child(span_6, true);
			reset(span_6);
			template_effect(() => {
				set_attribute(span_6, "title", get(selected).last_invalid_row_error);
				set_text(text_13, get(selected).last_invalid_row_error);
			});
			append($$anchor, span_6);
		};
		if_block(node_4, ($$render) => {
			if (get(selected).last_invalid_row_error) $$render(consequent_2);
		});
		reset(div_3);
		template_effect(($0, $1, $2, $3) => {
			set_text(text_9, $0);
			set_text(text_10, $1);
			set_text(text_11, $2);
			set_text(text_12, $3);
		}, [
			() => formatNumber(get(selected).last_price),
			() => formatNumber(get(selected).last_volume),
			() => formatNumber(get(selected).last_open_interest),
			() => formatNumber(get(selected).invalid_rows)
		]);
		append($$anchor, div_3);
	};
	if_block(node_3, ($$render) => {
		if (get(selected)) $$render(consequent_3);
	});
	reset(section);
	template_effect(() => set_text(text, `${get(ordered).length ?? ""} 条`));
	append($$anchor, section);
	pop();
}
delegate(["click"]);
//#endregion
//#region src/lib/api.ts
var DashboardApiError = class extends Error {
	path;
	status;
	constructor(path, status, message) {
		super(message);
		this.path = path;
		this.status = status;
	}
};
function symbolQueryString(filters) {
	const params = new URLSearchParams();
	if (filters.statuses.length > 0) params.set("status", filters.statuses.join(","));
	if (filters.subscribedOnly) params.set("subscribed", "1");
	if (filters.q.trim()) params.set("q", filters.q.trim());
	params.set("sort", filters.sort);
	params.set("limit", String(filters.limit));
	return params.toString();
}
async function fetchJson(path, signal) {
	const response = await fetch(path, {
		cache: "no-store",
		signal
	});
	const body = await response.json().catch(() => ({}));
	if (!response.ok) {
		const message = typeof body === "object" && body != null && "error" in body && typeof body.error === "string" ? body.error : `HTTP ${response.status}`;
		throw new DashboardApiError(path, response.status, message);
	}
	return body;
}
async function fetchRelaySnapshot(filters, signal) {
	const snapshot = await fetchJson(`/dashboard-snapshot?${symbolQueryString(filters)}`, signal);
	return {
		...snapshot,
		receivedAt: snapshot.received_at_unix_millis || Date.now()
	};
}
//#endregion
//#region src/lib/incident-ledger.ts
function createIncidentLedger(limit = 80) {
	return {
		limit,
		knownStatuses: /* @__PURE__ */ new Map(),
		incidents: []
	};
}
function severityForIncident(row) {
	if (row.problem_severity === "bad") return "bad";
	if (row.problem_severity === "warn") return "warn";
	if (row.problem_severity === "closed") return "closed";
	return "live";
}
function updateIncidentLedger(ledger, model) {
	for (const row of model.globalRows) {
		const before = ledger.knownStatuses.get(row.symbol);
		if (before && before !== row.status) {
			const incident = {
				id: `${model.sampledAt}:${row.symbol}:${before}:${row.status}`,
				at: model.sampledAt,
				scope: row.instrument_name ?? row.symbol,
				scope_symbol: row.symbol,
				type: statusLabel(row.status),
				detail: `${statusLabel(before)} -> ${statusLabel(row.status)}`,
				impact: row.subscribed ? "影响订阅" : "未订阅",
				severity: severityForIncident(row)
			};
			if (!ledger.incidents.some((item) => item.id === incident.id)) ledger.incidents.unshift(incident);
		}
		ledger.knownStatuses.set(row.symbol, row.status);
	}
	if (ledger.incidents.length > ledger.limit) ledger.incidents.splice(ledger.limit);
	return ledger;
}
//#endregion
//#region src/App.svelte
var root = /* @__PURE__ */ from_html(`<!> <section class="kpi-grid" aria-label="relay metrics"><!> <!> <!> <!> <!></section> <!> <section class="dashboard-main"><!> <!> <!></section> <!>`, 1);
var root_1 = /* @__PURE__ */ from_html(`<section class="panel grid min-h-[280px] place-content-center text-center text-[var(--relay-muted)]">正在读取 relay 观测数据</section>`);
var root_2 = /* @__PURE__ */ from_html(`<main class="dashboard-shell"><!> <!> <!></main>`);
function App($$anchor, $$props) {
	push($$props, true);
	const POLL_INTERVAL_MS = 2e3;
	let snapshot = /* @__PURE__ */ state(null);
	let model = /* @__PURE__ */ state(null);
	let timeline = proxy(createTimelineHistory());
	let incidents = proxy(createIncidentLedger());
	let error = /* @__PURE__ */ state(null);
	let sequence = /* @__PURE__ */ state(0);
	let view = proxy({
		paused: false,
		fullscreen: false,
		selectedExchange: null,
		selectedSymbol: null,
		filters: {
			statuses: [],
			subscribedOnly: false,
			q: "",
			sort: "receive_gap_ms_desc",
			limit: 200
		}
	});
	let buckets = /* @__PURE__ */ user_derived(() => timelineBuckets(timeline, get(snapshot)?.receivedAt ?? Date.now(), 60));
	let filterKey = /* @__PURE__ */ user_derived(() => JSON.stringify(view.filters));
	async function load(signal) {
		const requestId = get(sequence) + 1;
		set(sequence, requestId);
		const next = await fetchRelaySnapshot(view.filters, signal);
		if (requestId !== get(sequence)) return;
		set(snapshot, next, true);
		const nextModel = deriveIntegrity(next.metrics, next.page, next.receivedAt, get(model), next.global, next.global_symbols);
		pushTimelineSample(timeline, nextModel);
		updateIncidentLedger(incidents, nextModel);
		set(model, nextModel, true);
		set(error, null);
	}
	function refreshNow() {
		return load();
	}
	user_effect(() => {
		get(filterKey);
		if (view.paused) return;
		const controller = new AbortController();
		let disposed = false;
		let timer;
		async function poll() {
			try {
				await untrack(() => load(controller.signal));
			} catch (reason) {
				if (!controller.signal.aborted) set(error, reason instanceof Error ? reason.message : String(reason), true);
			}
			if (!disposed && !controller.signal.aborted) timer = window.setTimeout(poll, POLL_INTERVAL_MS);
		}
		poll();
		return () => {
			disposed = true;
			controller.abort();
			if (timer != null) window.clearTimeout(timer);
		};
	});
	var main = root_2();
	var node = child(main);
	MonitorHeader(node, {
		get model() {
			return get(model);
		},
		get error() {
			return get(error);
		},
		get paused() {
			return view.paused;
		},
		set paused($$value) {
			view.paused = $$value;
		},
		get fullscreen() {
			return view.fullscreen;
		},
		set fullscreen($$value) {
			view.fullscreen = $$value;
		}
	});
	var node_1 = sibling(node, 2);
	DashboardControls(node_1, {
		get disabled() {
			return view.paused;
		},
		onrefresh: refreshNow,
		get filters() {
			return view.filters;
		},
		set filters($$value) {
			view.filters = $$value;
		}
	});
	var node_2 = sibling(node_1, 2);
	var consequent = ($$anchor) => {
		var fragment = root();
		var node_3 = first_child(fragment);
		IntegrityHero(node_3, { get model() {
			return get(model);
		} });
		var section = sibling(node_3, 2);
		var node_4 = child(section);
		MetricCard(node_4, {
			label: "上游帧流",
			get value() {
				return get(model).frameRate;
			},
			unit: "/s",
			tone: "info",
			format: "rate",
			icon: "⌁"
		});
		var node_5 = sibling(node_4, 2);
		MetricCard(node_5, {
			label: "有效事件",
			get value() {
				return get(model).eventRate;
			},
			unit: "/s",
			tone: "accent",
			format: "rate",
			icon: "▥"
		});
		var node_6 = sibling(node_5, 2);
		{
			let $0 = /* @__PURE__ */ user_derived(() => get(model).coverageRatio * 100);
			MetricCard(node_6, {
				label: "合约覆盖",
				get value() {
					return get($0);
				},
				unit: "%",
				tone: "live",
				format: "percent",
				icon: "◎"
			});
		}
		var node_7 = sibling(node_6, 2);
		MetricCard(node_7, {
			label: "Diff行号",
			get value() {
				return get(model).diffRowDiscontinuityCount;
			},
			tone: "info",
			icon: "⌁"
		});
		var node_8 = sibling(node_7, 2);
		{
			let $0 = /* @__PURE__ */ user_derived(() => get(model).frameFlowHealth === "critical" ? "bad" : get(model).frameFlowHealth === "warn" ? "warn" : "info");
			MetricCard(node_8, {
				label: "上游帧静默",
				get value() {
					return get(model).upstreamIdleMs;
				},
				format: "duration",
				get tone() {
					return get($0);
				},
				icon: "↻"
			});
		}
		reset(section);
		var node_9 = sibling(section, 2);
		RelayPipeline(node_9, { get model() {
			return get(model);
		} });
		var section_1 = sibling(node_9, 2);
		var node_10 = child(section_1);
		AttentionList(node_10, { get rows() {
			return get(model).problems;
		} });
		var node_11 = sibling(node_10, 2);
		ContinuityTimeline(node_11, {
			get buckets() {
				return get(buckets);
			},
			get rows() {
				return get(model).globalRows;
			}
		});
		IncidentTable(sibling(node_11, 2), { get incidents() {
			return incidents.incidents;
		} });
		reset(section_1);
		SymbolHealthTable(sibling(section_1, 2), {
			get rows() {
				return get(model).rows;
			},
			get selectedSymbol() {
				return view.selectedSymbol;
			},
			set selectedSymbol($$value) {
				view.selectedSymbol = $$value;
			}
		});
		append($$anchor, fragment);
	};
	var alternate = ($$anchor) => {
		append($$anchor, root_1());
	};
	if_block(node_2, ($$render) => {
		if (get(model)) $$render(consequent);
		else $$render(alternate, -1);
	});
	reset(main);
	template_effect(() => set_attribute(main, "data-fullscreen", view.fullscreen));
	append($$anchor, main);
	pop();
}
//#endregion
//#region src/main.ts
var target = document.getElementById("app");
if (!target) throw new Error("dashboard root #app not found");
mount(App, { target });
//#endregion
