/* Zepto v1.0-1-ga3cab6c - polyfill zepto detect event ajax form fx - zeptojs.com/license */
(function(a){String.prototype.trim===a&&(String.prototype.trim=function(){return this.replace(/^\s+|\s+$/g,"")}),Array.prototype.reduce===a&&(Array.prototype.reduce=function(b){if(this===void 0||this===null)throw new TypeError;var c=Object(this),d=c.length>>>0,e=0,f;if(typeof b!="function")throw new TypeError;if(d==0&&arguments.length==1)throw new TypeError;if(arguments.length>=2)f=arguments[1];else do{if(e in c){f=c[e++];break}if(++e>=d)throw new TypeError}while(!0);while(e<d)e in c&&(f=b.call(a,f,c[e],e,c)),e++;return f})})();var Zepto=function(){function E(a){return a==null?String(a):y[z.call(a)]||"object"}function F(a){return E(a)=="function"}function G(a){return a!=null&&a==a.window}function H(a){return a!=null&&a.nodeType==a.DOCUMENT_NODE}function I(a){return E(a)=="object"}function J(a){return I(a)&&!G(a)&&a.__proto__==Object.prototype}function K(a){return a instanceof Array}function L(a){return typeof a.length=="number"}function M(a){return g.call(a,function(a){return a!=null})}function N(a){return a.length>0?c.fn.concat.apply([],a):a}function O(a){return a.replace(/::/g,"/").replace(/([A-Z]+)([A-Z][a-z])/g,"$1_$2").replace(/([a-z\d])([A-Z])/g,"$1_$2").replace(/_/g,"-").toLowerCase()}function P(a){return a in j?j[a]:j[a]=new RegExp("(^|\\s)"+a+"(\\s|$)")}function Q(a,b){return typeof b=="number"&&!l[O(a)]?b+"px":b}function R(a){var b,c;return i[a]||(b=h.createElement(a),h.body.appendChild(b),c=k(b,"").getPropertyValue("display"),b.parentNode.removeChild(b),c=="none"&&(c="block"),i[a]=c),i[a]}function S(a){return"children"in a?f.call(a.children):c.map(a.childNodes,function(a){if(a.nodeType==1)return a})}function T(c,d,e){for(b in d)e&&(J(d[b])||K(d[b]))?(J(d[b])&&!J(c[b])&&(c[b]={}),K(d[b])&&!K(c[b])&&(c[b]=[]),T(c[b],d[b],e)):d[b]!==a&&(c[b]=d[b])}function U(b,d){return d===a?c(b):c(b).filter(d)}function V(a,b,c,d){return F(b)?b.call(a,c,d):b}function W(a,b,c){c==null?a.removeAttribute(b):a.setAttribute(b,c)}function X(b,c){var d=b.className,e=d&&d.baseVal!==a;if(c===a)return e?d.baseVal:d;e?d.baseVal=c:b.className=c}function Y(a){var b;try{return a?a=="true"||(a=="false"?!1:a=="null"?null:isNaN(b=Number(a))?/^[\[\{]/.test(a)?c.parseJSON(a):a:b):a}catch(d){return a}}function Z(a,b){b(a);for(var c in a.childNodes)Z(a.childNodes[c],b)}var a,b,c,d,e=[],f=e.slice,g=e.filter,h=window.document,i={},j={},k=h.defaultView.getComputedStyle,l={"column-count":1,columns:1,"font-weight":1,"line-height":1,opacity:1,"z-index":1,zoom:1},m=/^\s*<(\w+|!)[^>]*>/,n=/<(?!area|br|col|embed|hr|img|input|link|meta|param)(([\w:]+)[^>]*)\/>/ig,o=/^(?:body|html)$/i,p=["val","css","html","text","data","width","height","offset"],q=["after","prepend","before","append"],r=h.createElement("table"),s=h.createElement("tr"),t={tr:h.createElement("tbody"),tbody:r,thead:r,tfoot:r,td:s,th:s,"*":h.createElement("div")},u=/complete|loaded|interactive/,v=/^\.([\w-]+)$/,w=/^#([\w-]*)$/,x=/^[\w-]+$/,y={},z=y.toString,A={},B,C,D=h.createElement("div");return A.matches=function(a,b){if(!a||a.nodeType!==1)return!1;var c=a.webkitMatchesSelector||a.mozMatchesSelector||a.oMatchesSelector||a.matchesSelector;if(c)return c.call(a,b);var d,e=a.parentNode,f=!e;return f&&(e=D).appendChild(a),d=~A.qsa(e,b).indexOf(a),f&&D.removeChild(a),d},B=function(a){return a.replace(/-+(.)?/g,function(a,b){return b?b.toUpperCase():""})},C=function(a){return g.call(a,function(b,c){return a.indexOf(b)==c})},A.fragment=function(b,d,e){b.replace&&(b=b.replace(n,"<$1></$2>")),d===a&&(d=m.test(b)&&RegExp.$1),d in t||(d="*");var g,h,i=t[d];return i.innerHTML=""+b,h=c.each(f.call(i.childNodes),function(){i.removeChild(this)}),J(e)&&(g=c(h),c.each(e,function(a,b){p.indexOf(a)>-1?g[a](b):g.attr(a,b)})),h},A.Z=function(a,b){return a=a||[],a.__proto__=c.fn,a.selector=b||"",a},A.isZ=function(a){return a instanceof A.Z},A.init=function(b,d){if(!b)return A.Z();if(F(b))return c(h).ready(b);if(A.isZ(b))return b;var e;if(K(b))e=M(b);else if(I(b))e=[J(b)?c.extend({},b):b],b=null;else if(m.test(b))e=A.fragment(b.trim(),RegExp.$1,d),b=null;else{if(d!==a)return c(d).find(b);e=A.qsa(h,b)}return A.Z(e,b)},c=function(a,b){return A.init(a,b)},c.extend=function(a){var b,c=f.call(arguments,1);return typeof a=="boolean"&&(b=a,a=c.shift()),c.forEach(function(c){T(a,c,b)}),a},A.qsa=function(a,b){var c;return H(a)&&w.test(b)?(c=a.getElementById(RegExp.$1))?[c]:[]:a.nodeType!==1&&a.nodeType!==9?[]:f.call(v.test(b)?a.getElementsByClassName(RegExp.$1):x.test(b)?a.getElementsByTagName(b):a.querySelectorAll(b))},c.contains=function(a,b){return a!==b&&a.contains(b)},c.type=E,c.isFunction=F,c.isWindow=G,c.isArray=K,c.isPlainObject=J,c.isEmptyObject=function(a){var b;for(b in a)return!1;return!0},c.inArray=function(a,b,c){return e.indexOf.call(b,a,c)},c.camelCase=B,c.trim=function(a){return a.trim()},c.uuid=0,c.support={},c.expr={},c.map=function(a,b){var c,d=[],e,f;if(L(a))for(e=0;e<a.length;e++)c=b(a[e],e),c!=null&&d.push(c);else for(f in a)c=b(a[f],f),c!=null&&d.push(c);return N(d)},c.each=function(a,b){var c,d;if(L(a)){for(c=0;c<a.length;c++)if(b.call(a[c],c,a[c])===!1)return a}else for(d in a)if(b.call(a[d],d,a[d])===!1)return a;return a},c.grep=function(a,b){return g.call(a,b)},window.JSON&&(c.parseJSON=JSON.parse),c.each("Boolean Number String Function Array Date RegExp Object Error".split(" "),function(a,b){y["[object "+b+"]"]=b.toLowerCase()}),c.fn={forEach:e.forEach,reduce:e.reduce,push:e.push,sort:e.sort,indexOf:e.indexOf,concat:e.concat,map:function(a){return c(c.map(this,function(b,c){return a.call(b,c,b)}))},slice:function(){return c(f.apply(this,arguments))},ready:function(a){return u.test(h.readyState)?a(c):h.addEventListener("DOMContentLoaded",function(){a(c)},!1),this},get:function(b){return b===a?f.call(this):this[b>=0?b:b+this.length]},toArray:function(){return this.get()},size:function(){return this.length},remove:function(){return this.each(function(){this.parentNode!=null&&this.parentNode.removeChild(this)})},each:function(a){return e.every.call(this,function(b,c){return a.call(b,c,b)!==!1}),this},filter:function(a){return F(a)?this.not(this.not(a)):c(g.call(this,function(b){return A.matches(b,a)}))},add:function(a,b){return c(C(this.concat(c(a,b))))},is:function(a){return this.length>0&&A.matches(this[0],a)},not:function(b){var d=[];if(F(b)&&b.call!==a)this.each(function(a){b.call(this,a)||d.push(this)});else{var e=typeof b=="string"?this.filter(b):L(b)&&F(b.item)?f.call(b):c(b);this.forEach(function(a){e.indexOf(a)<0&&d.push(a)})}return c(d)},has:function(a){return this.filter(function(){return I(a)?c.contains(this,a):c(this).find(a).size()})},eq:function(a){return a===-1?this.slice(a):this.slice(a,+a+1)},first:function(){var a=this[0];return a&&!I(a)?a:c(a)},last:function(){var a=this[this.length-1];return a&&!I(a)?a:c(a)},find:function(a){var b,d=this;return typeof a=="object"?b=c(a).filter(function(){var a=this;return e.some.call(d,function(b){return c.contains(b,a)})}):this.length==1?b=c(A.qsa(this[0],a)):b=this.map(function(){return A.qsa(this,a)}),b},closest:function(a,b){var d=this[0],e=!1;typeof a=="object"&&(e=c(a));while(d&&!(e?e.indexOf(d)>=0:A.matches(d,a)))d=d!==b&&!H(d)&&d.parentNode;return c(d)},parents:function(a){var b=[],d=this;while(d.length>0)d=c.map(d,function(a){if((a=a.parentNode)&&!H(a)&&b.indexOf(a)<0)return b.push(a),a});return U(b,a)},parent:function(a){return U(C(this.pluck("parentNode")),a)},children:function(a){return U(this.map(function(){return S(this)}),a)},contents:function(){return this.map(function(){return f.call(this.childNodes)})},siblings:function(a){return U(this.map(function(a,b){return g.call(S(b.parentNode),function(a){return a!==b})}),a)},empty:function(){return this.each(function(){this.innerHTML=""})},pluck:function(a){return c.map(this,function(b){return b[a]})},show:function(){return this.each(function(){this.style.display=="none"&&(this.style.display=null),k(this,"").getPropertyValue("display")=="none"&&(this.style.display=R(this.nodeName))})},replaceWith:function(a){return this.before(a).remove()},wrap:function(a){var b=F(a);if(this[0]&&!b)var d=c(a).get(0),e=d.parentNode||this.length>1;return this.each(function(f){c(this).wrapAll(b?a.call(this,f):e?d.cloneNode(!0):d)})},wrapAll:function(a){if(this[0]){c(this[0]).before(a=c(a));var b;while((b=a.children()).length)a=b.first();c(a).append(this)}return this},wrapInner:function(a){var b=F(a);return this.each(function(d){var e=c(this),f=e.contents(),g=b?a.call(this,d):a;f.length?f.wrapAll(g):e.append(g)})},unwrap:function(){return this.parent().each(function(){c(this).replaceWith(c(this).children())}),this},clone:function(){return this.map(function(){return this.cloneNode(!0)})},hide:function(){return this.css("display","none")},toggle:function(b){return this.each(function(){var d=c(this);(b===a?d.css("display")=="none":b)?d.show():d.hide()})},prev:function(a){return c(this.pluck("previousElementSibling")).filter(a||"*")},next:function(a){return c(this.pluck("nextElementSibling")).filter(a||"*")},html:function(b){return b===a?this.length>0?this[0].innerHTML:null:this.each(function(a){var d=this.innerHTML;c(this).empty().append(V(this,b,a,d))})},text:function(b){return b===a?this.length>0?this[0].textContent:null:this.each(function(){this.textContent=b})},attr:function(c,d){var e;return typeof c=="string"&&d===a?this.length==0||this[0].nodeType!==1?a:c=="value"&&this[0].nodeName=="INPUT"?this.val():!(e=this[0].getAttribute(c))&&c in this[0]?this[0][c]:e:this.each(function(a){if(this.nodeType!==1)return;if(I(c))for(b in c)W(this,b,c[b]);else W(this,c,V(this,d,a,this.getAttribute(c)))})},removeAttr:function(a){return this.each(function(){this.nodeType===1&&W(this,a)})},prop:function(b,c){return c===a?this[0]&&this[0][b]:this.each(function(a){this[b]=V(this,c,a,this[b])})},data:function(b,c){var d=this.attr("data-"+O(b),c);return d!==null?Y(d):a},val:function(b){return b===a?this[0]&&(this[0].multiple?c(this[0]).find("option").filter(function(a){return this.selected}).pluck("value"):this[0].value):this.each(function(a){this.value=V(this,b,a,this.value)})},offset:function(a){if(a)return this.each(function(b){var d=c(this),e=V(this,a,b,d.offset()),f=d.offsetParent().offset(),g={top:e.top-f.top,left:e.left-f.left};d.css("position")=="static"&&(g.position="relative"),d.css(g)});if(this.length==0)return null;var b=this[0].getBoundingClientRect();return{left:b.left+window.pageXOffset,top:b.top+window.pageYOffset,width:Math.round(b.width),height:Math.round(b.height)}},css:function(a,c){if(arguments.length<2&&typeof a=="string")return this[0]&&(this[0].style[B(a)]||k(this[0],"").getPropertyValue(a));var d="";if(E(a)=="string")!c&&c!==0?this.each(function(){this.style.removeProperty(O(a))}):d=O(a)+":"+Q(a,c);else for(b in a)!a[b]&&a[b]!==0?this.each(function(){this.style.removeProperty(O(b))}):d+=O(b)+":"+Q(b,a[b])+";";return this.each(function(){this.style.cssText+=";"+d})},index:function(a){return a?this.indexOf(c(a)[0]):this.parent().children().indexOf(this[0])},hasClass:function(a){return e.some.call(this,function(a){return this.test(X(a))},P(a))},addClass:function(a){return this.each(function(b){d=[];var e=X(this),f=V(this,a,b,e);f.split(/\s+/g).forEach(function(a){c(this).hasClass(a)||d.push(a)},this),d.length&&X(this,e+(e?" ":"")+d.join(" "))})},removeClass:function(b){return this.each(function(c){if(b===a)return X(this,"");d=X(this),V(this,b,c,d).split(/\s+/g).forEach(function(a){d=d.replace(P(a)," ")}),X(this,d.trim())})},toggleClass:function(b,d){return this.each(function(e){var f=c(this),g=V(this,b,e,X(this));g.split(/\s+/g).forEach(function(b){(d===a?!f.hasClass(b):d)?f.addClass(b):f.removeClass(b)})})},scrollTop:function(){if(!this.length)return;return"scrollTop"in this[0]?this[0].scrollTop:this[0].scrollY},position:function(){if(!this.length)return;var a=this[0],b=this.offsetParent(),d=this.offset(),e=o.test(b[0].nodeName)?{top:0,left:0}:b.offset();return d.top-=parseFloat(c(a).css("margin-top"))||0,d.left-=parseFloat(c(a).css("margin-left"))||0,e.top+=parseFloat(c(b[0]).css("border-top-width"))||0,e.left+=parseFloat(c(b[0]).css("border-left-width"))||0,{top:d.top-e.top,left:d.left-e.left}},offsetParent:function(){return this.map(function(){var a=this.offsetParent||h.body;while(a&&!o.test(a.nodeName)&&c(a).css("position")=="static")a=a.offsetParent;return a})}},c.fn.detach=c.fn.remove,["width","height"].forEach(function(b){c.fn[b]=function(d){var e,f=this[0],g=b.replace(/./,function(a){return a[0].toUpperCase()});return d===a?G(f)?f["inner"+g]:H(f)?f.documentElement["offset"+g]:(e=this.offset())&&e[b]:this.each(function(a){f=c(this),f.css(b,V(this,d,a,f[b]()))})}}),q.forEach(function(a,b){var d=b%2;c.fn[a]=function(){var a,e=c.map(arguments,function(b){return a=E(b),a=="object"||a=="array"||b==null?b:A.fragment(b)}),f,g=this.length>1;return e.length<1?this:this.each(function(a,h){f=d?h:h.parentNode,h=b==0?h.nextSibling:b==1?h.firstChild:b==2?h:null,e.forEach(function(a){if(g)a=a.cloneNode(!0);else if(!f)return c(a).remove();Z(f.insertBefore(a,h),function(a){a.nodeName!=null&&a.nodeName.toUpperCase()==="SCRIPT"&&(!a.type||a.type==="text/javascript")&&!a.src&&window.eval.call(window,a.innerHTML)})})})},c.fn[d?a+"To":"insert"+(b?"Before":"After")]=function(b){return c(b)[a](this),this}}),A.Z.prototype=c.fn,A.uniq=C,A.deserializeValue=Y,c.zepto=A,c}();window.Zepto=Zepto,"$"in window||(window.$=Zepto),function(a){function b(a){var b=this.os={},c=this.browser={},d=a.match(/WebKit\/([\d.]+)/),e=a.match(/(Android)\s+([\d.]+)/),f=a.match(/(iPad).*OS\s([\d_]+)/),g=!f&&a.match(/(iPhone\sOS)\s([\d_]+)/),h=a.match(/(webOS|hpwOS)[\s\/]([\d.]+)/),i=h&&a.match(/TouchPad/),j=a.match(/Kindle\/([\d.]+)/),k=a.match(/Silk\/([\d._]+)/),l=a.match(/(BlackBerry).*Version\/([\d.]+)/),m=a.match(/(BB10).*Version\/([\d.]+)/),n=a.match(/(RIM\sTablet\sOS)\s([\d.]+)/),o=a.match(/PlayBook/),p=a.match(/Chrome\/([\d.]+)/)||a.match(/CriOS\/([\d.]+)/),q=a.match(/Firefox\/([\d.]+)/);if(c.webkit=!!d)c.version=d[1];e&&(b.android=!0,b.version=e[2]),g&&(b.ios=b.iphone=!0,b.version=g[2].replace(/_/g,".")),f&&(b.ios=b.ipad=!0,b.version=f[2].replace(/_/g,".")),h&&(b.webos=!0,b.version=h[2]),i&&(b.touchpad=!0),l&&(b.blackberry=!0,b.version=l[2]),m&&(b.bb10=!0,b.version=m[2]),n&&(b.rimtabletos=!0,b.version=n[2]),o&&(c.playbook=!0),j&&(b.kindle=!0,b.version=j[1]),k&&(c.silk=!0,c.version=k[1]),!k&&b.android&&a.match(/Kindle Fire/)&&(c.silk=!0),p&&(c.chrome=!0,c.version=p[1]),q&&(c.firefox=!0,c.version=q[1]),b.tablet=!!(f||o||e&&!a.match(/Mobile/)||q&&a.match(/Tablet/)),b.phone=!b.tablet&&!!(e||g||h||l||m||p&&a.match(/Android/)||p&&a.match(/CriOS\/([\d.]+)/)||q&&a.match(/Mobile/))}b.call(a,navigator.userAgent),a.__detect=b}(Zepto),function(a){function g(a){return a._zid||(a._zid=d++)}function h(a,b,d,e){b=i(b);if(b.ns)var f=j(b.ns);return(c[g(a)]||[]).filter(function(a){return a&&(!b.e||a.e==b.e)&&(!b.ns||f.test(a.ns))&&(!d||g(a.fn)===g(d))&&(!e||a.sel==e)})}function i(a){var b=(""+a).split(".");return{e:b[0],ns:b.slice(1).sort().join(" ")}}function j(a){return new RegExp("(?:^| )"+a.replace(" "," .* ?")+"(?: |$)")}function k(b,c,d){a.type(b)!="string"?a.each(b,d):b.split(/\s/).forEach(function(a){d(a,c)})}function l(a,b){return a.del&&(a.e=="focus"||a.e=="blur")||!!b}function m(a){return f[a]||a}function n(b,d,e,h,j,n){var o=g(b),p=c[o]||(c[o]=[]);k(d,e,function(c,d){var e=i(c);e.fn=d,e.sel=h,e.e in f&&(d=function(b){var c=b.relatedTarget;if(!c||c!==this&&!a.contains(this,c))return e.fn.apply(this,arguments)}),e.del=j&&j(d,c);var g=e.del||d;e.proxy=function(a){var c=g.apply(b,[a].concat(a.data));return c===!1&&(a.preventDefault(),a.stopPropagation()),c},e.i=p.length,p.push(e),b.addEventListener(m(e.e),e.proxy,l(e,n))})}function o(a,b,d,e,f){var i=g(a);k(b||"",d,function(b,d){h(a,b,d,e).forEach(function(b){delete c[i][b.i],a.removeEventListener(m(b.e),b.proxy,l(b,f))})})}function t(b){var c,d={originalEvent:b};for(c in b)!r.test(c)&&b[c]!==undefined&&(d[c]=b[c]);return a.each(s,function(a,c){d[a]=function(){return this[c]=p,b[a].apply(b,arguments)},d[c]=q}),d}function u(a){if(!("defaultPrevented"in a)){a.defaultPrevented=!1;var b=a.preventDefault;a.preventDefault=function(){this.defaultPrevented=!0,b.call(this)}}}var b=a.zepto.qsa,c={},d=1,e={},f={mouseenter:"mouseover",mouseleave:"mouseout"};e.click=e.mousedown=e.mouseup=e.mousemove="MouseEvents",a.event={add:n,remove:o},a.proxy=function(b,c){if(a.isFunction(b)){var d=function(){return b.apply(c,arguments)};return d._zid=g(b),d}if(typeof c=="string")return a.proxy(b[c],b);throw new TypeError("expected function")},a.fn.bind=function(a,b){return this.each(function(){n(this,a,b)})},a.fn.unbind=function(a,b){return this.each(function(){o(this,a,b)})},a.fn.one=function(a,b){return this.each(function(c,d){n(this,a,b,null,function(a,b){return function(){var c=a.apply(d,arguments);return o(d,b,a),c}})})};var p=function(){return!0},q=function(){return!1},r=/^([A-Z]|layer[XY]$)/,s={preventDefault:"isDefaultPrevented",stopImmediatePropagation:"isImmediatePropagationStopped",stopPropagation:"isPropagationStopped"};a.fn.delegate=function(b,c,d){return this.each(function(e,f){n(f,c,d,b,function(c){return function(d){var e,g=a(d.target).closest(b,f).get(0);if(g)return e=a.extend(t(d),{currentTarget:g,liveFired:f}),c.apply(g,[e].concat([].slice.call(arguments,1)))}})})},a.fn.undelegate=function(a,b,c){return this.each(function(){o(this,b,c,a)})},a.fn.live=function(b,c){return a(document.body).delegate(this.selector,b,c),this},a.fn.die=function(b,c){return a(document.body).undelegate(this.selector,b,c),this},a.fn.on=function(b,c,d){return!c||a.isFunction(c)?this.bind(b,c||d):this.delegate(c,b,d)},a.fn.off=function(b,c,d){return!c||a.isFunction(c)?this.unbind(b,c||d):this.undelegate(c,b,d)},a.fn.trigger=function(b,c){if(typeof b=="string"||a.isPlainObject(b))b=a.Event(b);return u(b),b.data=c,this.each(function(){"dispatchEvent"in this&&this.dispatchEvent(b)})},a.fn.triggerHandler=function(b,c){var d,e;return this.each(function(f,g){d=t(typeof b=="string"?a.Event(b):b),d.data=c,d.target=g,a.each(h(g,b.type||b),function(a,b){e=b.proxy(d);if(d.isImmediatePropagationStopped())return!1})}),e},"focusin focusout load resize scroll unload click dblclick mousedown mouseup mousemove mouseover mouseout mouseenter mouseleave change select keydown keypress keyup error".split(" ").forEach(function(b){a.fn[b]=function(a){return a?this.bind(b,a):this.trigger(b)}}),["focus","blur"].forEach(function(b){a.fn[b]=function(a){return a?this.bind(b,a):this.each(function(){try{this[b]()}catch(a){}}),this}}),a.Event=function(a,b){typeof a!="string"&&(b=a,a=b.type);var c=document.createEvent(e[a]||"Events"),d=!0;if(b)for(var f in b)f=="bubbles"?d=!!b[f]:c[f]=b[f];return c.initEvent(a,d,!0,null,null,null,null,null,null,null,null,null,null,null,null),c.isDefaultPrevented=function(){return this.defaultPrevented},c}}(Zepto),function($){function triggerAndReturn(a,b,c){var d=$.Event(b);return $(a).trigger(d,c),!d.defaultPrevented}function triggerGlobal(a,b,c,d){if(a.global)return triggerAndReturn(b||document,c,d)}function ajaxStart(a){a.global&&$.active++===0&&triggerGlobal(a,null,"ajaxStart")}function ajaxStop(a){a.global&&!--$.active&&triggerGlobal(a,null,"ajaxStop")}function ajaxBeforeSend(a,b){var c=b.context;if(b.beforeSend.call(c,a,b)===!1||triggerGlobal(b,c,"ajaxBeforeSend",[a,b])===!1)return!1;triggerGlobal(b,c,"ajaxSend",[a,b])}function ajaxSuccess(a,b,c){var d=c.context,e="success";c.success.call(d,a,e,b),triggerGlobal(c,d,"ajaxSuccess",[b,c,a]),ajaxComplete(e,b,c)}function ajaxError(a,b,c,d){var e=d.context;d.error.call(e,c,b,a),triggerGlobal(d,e,"ajaxError",[c,d,a]),ajaxComplete(b,c,d)}function ajaxComplete(a,b,c){var d=c.context;c.complete.call(d,b,a),triggerGlobal(c,d,"ajaxComplete",[b,c]),ajaxStop(c)}function empty(){}function mimeToDataType(a){return a&&(a=a.split(";",2)[0]),a&&(a==htmlType?"html":a==jsonType?"json":scriptTypeRE.test(a)?"script":xmlTypeRE.test(a)&&"xml")||"text"}function appendQuery(a,b){return(a+"&"+b).replace(/[&?]{1,2}/,"?")}function serializeData(a){a.processData&&a.data&&$.type(a.data)!="string"&&(a.data=$.param(a.data,a.traditional)),a.data&&(!a.type||a.type.toUpperCase()=="GET")&&(a.url=appendQuery(a.url,a.data))}function parseArguments(a,b,c,d){var e=!$.isFunction(b);return{url:a,data:e?b:undefined,success:e?$.isFunction(c)?c:undefined:b,dataType:e?d||c:c}}function serialize(a,b,c,d){var e,f=$.isArray(b);$.each(b,function(b,g){e=$.type(g),d&&(b=c?d:d+"["+(f?"":b)+"]"),!d&&f?a.add(g.name,g.value):e=="array"||!c&&e=="object"?serialize(a,g,c,b):a.add(b,g)})}var jsonpID=0,document=window.document,key,name,rscript=/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi,scriptTypeRE=/^(?:text|application)\/javascript/i,xmlTypeRE=/^(?:text|application)\/xml/i,jsonType="application/json",htmlType="text/html",blankRE=/^\s*$/;$.active=0,$.ajaxJSONP=function(a){if("type"in a){var b="jsonp"+ ++jsonpID,c=document.createElement("script"),d=function(){clearTimeout(g),$(c).remove(),delete window[b]},e=function(c){d();if(!c||c=="timeout")window[b]=empty;ajaxError(null,c||"abort",f,a)},f={abort:e},g;return ajaxBeforeSend(f,a)===!1?(e("abort"),!1):(window[b]=function(b){d(),ajaxSuccess(b,f,a)},c.onerror=function(){e("error")},c.src=a.url.replace(/=\?/,"="+b),$("head").append(c),a.timeout>0&&(g=setTimeout(function(){e("timeout")},a.timeout)),f)}return $.ajax(a)},$.ajaxSettings={type:"GET",beforeSend:empty,success:empty,error:empty,complete:empty,context:null,global:!0,xhr:function(){return new window.XMLHttpRequest},accepts:{script:"text/javascript, application/javascript",json:jsonType,xml:"application/xml, text/xml",html:htmlType,text:"text/plain"},crossDomain:!1,timeout:0,processData:!0,cache:!0},$.ajax=function(options){var settings=$.extend({},options||{});for(key in $.ajaxSettings)settings[key]===undefined&&(settings[key]=$.ajaxSettings[key]);ajaxStart(settings),settings.crossDomain||(settings.crossDomain=/^([\w-]+:)?\/\/([^\/]+)/.test(settings.url)&&RegExp.$2!=window.location.host),settings.url||(settings.url=window.location.toString()),serializeData(settings),settings.cache===!1&&(settings.url=appendQuery(settings.url,"_="+Date.now()));var dataType=settings.dataType,hasPlaceholder=/=\?/.test(settings.url);if(dataType=="jsonp"||hasPlaceholder)return hasPlaceholder||(settings.url=appendQuery(settings.url,"callback=?")),$.ajaxJSONP(settings);var mime=settings.accepts[dataType],baseHeaders={},protocol=/^([\w-]+:)\/\//.test(settings.url)?RegExp.$1:window.location.protocol,xhr=settings.xhr(),abortTimeout;settings.crossDomain||(baseHeaders["X-Requested-With"]="XMLHttpRequest"),mime&&(baseHeaders.Accept=mime,mime.indexOf(",")>-1&&(mime=mime.split(",",2)[0]),xhr.overrideMimeType&&xhr.overrideMimeType(mime));if(settings.contentType||settings.contentType!==!1&&settings.data&&settings.type.toUpperCase()!="GET")baseHeaders["Content-Type"]=settings.contentType||"application/x-www-form-urlencoded";settings.headers=$.extend(baseHeaders,settings.headers||{}),xhr.onreadystatechange=function(){if(xhr.readyState==4){xhr.onreadystatechange=empty,clearTimeout(abortTimeout);var result,error=!1;if(xhr.status>=200&&xhr.status<300||xhr.status==304||xhr.status==0&&protocol=="file:"){dataType=dataType||mimeToDataType(xhr.getResponseHeader("content-type")),result=xhr.responseText;try{dataType=="script"?(1,eval)(result):dataType=="xml"?result=xhr.responseXML:dataType=="json"&&(result=blankRE.test(result)?null:$.parseJSON(result))}catch(e){error=e}error?ajaxError(error,"parsererror",xhr,settings):ajaxSuccess(result,xhr,settings)}else ajaxError(null,xhr.status?"error":"abort",xhr,settings)}};var async="async"in settings?settings.async:!0;xhr.open(settings.type,settings.url,async);for(name in settings.headers)xhr.setRequestHeader(name,settings.headers[name]);return ajaxBeforeSend(xhr,settings)===!1?(xhr.abort(),!1):(settings.timeout>0&&(abortTimeout=setTimeout(function(){xhr.onreadystatechange=empty,xhr.abort(),ajaxError(null,"timeout",xhr,settings)},settings.timeout)),xhr.send(settings.data?settings.data:null),xhr)},$.get=function(a,b,c,d){return $.ajax(parseArguments.apply(null,arguments))},$.post=function(a,b,c,d){var e=parseArguments.apply(null,arguments);return e.type="POST",$.ajax(e)},$.getJSON=function(a,b,c){var d=parseArguments.apply(null,arguments);return d.dataType="json",$.ajax(d)},$.fn.load=function(a,b,c){if(!this.length)return this;var d=this,e=a.split(/\s/),f,g=parseArguments(a,b,c),h=g.success;return e.length>1&&(g.url=e[0],f=e[1]),g.success=function(a){d.html(f?$("<div>").html(a.replace(rscript,"")).find(f):a),h&&h.apply(d,arguments)},$.ajax(g),this};var escape=encodeURIComponent;$.param=function(a,b){var c=[];return c.add=function(a,b){this.push(escape(a)+"="+escape(b))},serialize(c,a,b),c.join("&").replace(/%20/g,"+")}}(Zepto),function(a){a.fn.serializeArray=function(){var b=[],c;return a(Array.prototype.slice.call(this.get(0).elements)).each(function(){c=a(this);var d=c.attr("type");this.nodeName.toLowerCase()!="fieldset"&&!this.disabled&&d!="submit"&&d!="reset"&&d!="button"&&(d!="radio"&&d!="checkbox"||this.checked)&&b.push({name:c.attr("name"),value:c.val()})}),b},a.fn.serialize=function(){var a=[];return this.serializeArray().forEach(function(b){a.push(encodeURIComponent(b.name)+"="+encodeURIComponent(b.value))}),a.join("&")},a.fn.submit=function(b){if(b)this.bind("submit",b);else if(this.length){var c=a.Event("submit");this.eq(0).trigger(c),c.defaultPrevented||this.get(0).submit()}return this}}(Zepto),function(a,b){function s(a){return t(a.replace(/([a-z])([A-Z])/,"$1-$2"))}function t(a){return a.toLowerCase()}function u(a){return d?d+a:t(a)}var c="",d,e,f,g={Webkit:"webkit",Moz:"",O:"o",ms:"MS"},h=window.document,i=h.createElement("div"),j=/^((translate|rotate|scale)(X|Y|Z|3d)?|matrix(3d)?|perspective|skew(X|Y)?)$/i,k,l,m,n,o,p,q,r={};a.each(g,function(a,e){if(i.style[a+"TransitionProperty"]!==b)return c="-"+t(a)+"-",d=e,!1}),k=c+"transform",r[l=c+"transition-property"]=r[m=c+"transition-duration"]=r[n=c+"transition-timing-function"]=r[o=c+"animation-name"]=r[p=c+"animation-duration"]=r[q=c+"animation-timing-function"]="",a.fx={off:d===b&&i.style.transitionProperty===b,speeds:{_default:400,fast:200,slow:600},cssPrefix:c,transitionEnd:u("TransitionEnd"),animationEnd:u("AnimationEnd")},a.fn.animate=function(b,c,d,e){return a.isPlainObject(c)&&(d=c.easing,e=c.complete,c=c.duration),c&&(c=(typeof c=="number"?c:a.fx.speeds[c]||a.fx.speeds._default)/1e3),this.anim(b,c,d,e)},a.fn.anim=function(c,d,e,f){var g,h={},i,t="",u=this,v,w=a.fx.transitionEnd;d===b&&(d=.4),a.fx.off&&(d=0);if(typeof c=="string")h[o]=c,h[p]=d+"s",h[q]=e||"linear",w=a.fx.animationEnd;else{i=[];for(g in c)j.test(g)?t+=g+"("+c[g]+") ":(h[g]=c[g],i.push(s(g)));t&&(h[k]=t,i.push(k)),d>0&&typeof c=="object"&&(h[l]=i.join(", "),h[m]=d+"s",h[n]=e||"linear")}return v=function(b){if(typeof b!="undefined"){if(b.target!==b.currentTarget)return;a(b.target).unbind(w,v)}a(this).css(r),f&&f.call(this)},d>0&&this.bind(w,v),this.size()&&this.get(0).clientLeft,this.css(h),d<=0&&setTimeout(function(){u.each(function(){v.call(this)})},0),this},i=null}(Zepto)// BERT-JS
//
// Copyright (c) Rusty Klophaus (@rklophaus)
//               Ben Browning (@bbrowning)
//               Maxim Sokhatsky (@5HT)

function BertClass() {
    this.BERT_START = String.fromCharCode(131);
    this.SMALL_ATOM = String.fromCharCode(115);
    this.ATOM = String.fromCharCode(100);
    this.BINARY = String.fromCharCode(109);
    this.SMALL_INTEGER = String.fromCharCode(97);
    this.INTEGER = String.fromCharCode(98);
    this.SMALL_BIG = String.fromCharCode(110);
    this.LARGE_BIG = String.fromCharCode(111);
    this.FLOAT = String.fromCharCode(99);
    this.STRING = String.fromCharCode(107);
    this.LIST = String.fromCharCode(108);
    this.SMALL_TUPLE = String.fromCharCode(104);
    this.LARGE_TUPLE = String.fromCharCode(105);
    this.NIL = String.fromCharCode(106);
    this.ZERO = String.fromCharCode(0);
}

function BertAtom(Obj) {
    this.type = "Atom";
    this.value = Obj;
    this.toString = function () { return Obj; };
}

function BertBinary(Obj) {
    this.type = "Binary";
    this.value = Obj;
    this.toString = function () { return "<<\"" + Obj + "\">>"; };
}

function BertTuple(Arr) {
    this.type = "Tuple";
    this.length = Arr.length;
    this.value = Arr;
    for (var i = 0; i < Arr.length; i++) { this[i] = Arr[i]; }
    this.toString = function () {
        var i, s = "";
        for (i = 0; i < this.length; i++) { if (s !== "") { s += ", "; } s += this[i].toString(); }
        return "{" + s + "}";
    };
}

// - INTERFACE -

BertClass.prototype.atom = function (Obj) { return new BertAtom(Obj); };
BertClass.prototype.binary = function (Obj) { return new BertBinary(Obj); };
BertClass.prototype.tuple = function () { return new BertTuple(arguments); };
BertClass.prototype.encode = function (Obj) { return this.BERT_START + this.encode_inner(Obj); };
BertClass.prototype.decode = function (S) {
    if (S[0] !== this.BERT_START) { throw ("Not a valid BERT."); }
    var Obj = this.decode_inner(S.substring(1));
    if (Obj.rest !== "") { throw ("Invalid BERT."); }
    return Obj.value;
};

BertClass.prototype.decodebuf = function (S) { return Bert.decode(Bert.bytes_to_string(new Uint8Array(S))); };
//BertClass.prototype.decodebuf = function (S) { return Bert.decode(utf8.toByteArray(S)); };
BertClass.prototype.encodebuf = function (S) { 
    var ori = Bert.encode(S);
    var buf = new Uint8Array(new ArrayBuffer(ori.length));
    for (var i=0;i<buf.length;i++) buf[i] = ori.charCodeAt(i);
    return buf
};


// - ENCODING -

BertClass.prototype.encode_inner = function (Obj) {
    var func = 'encode_' + typeof(Obj);
    return this[func](Obj);
};

BertClass.prototype.encode_string = function (Obj) { return this.STRING + this.int_to_bytes(Obj.length, 2) + Obj; };
BertClass.prototype.encode_boolean = function (Obj) {
    if (Obj) { return this.encode_inner(this.atom("true")); }
    else { return this.encode_inner(this.atom("false")); }
};

BertClass.prototype.encode_number = function (Obj) {
    var s, isInteger = (Obj % 1 === 0);
    if (!isInteger) { return this.encode_float(Obj); }
    if (isInteger && Obj >= 0 && Obj < 256) { return this.SMALL_INTEGER + this.int_to_bytes(Obj, 1); }
    if (isInteger && Obj >= -134217728 && Obj <= 134217727) { return this.INTEGER + this.int_to_bytes(Obj, 4); }
    s = this.bignum_to_bytes(Obj);
    if (s.length < 256) { return this.SMALL_BIG + this.int_to_bytes(s.length - 1, 1) + s; } 
    else { return this.LARGE_BIG + this.int_to_bytes(s.length - 1, 4) + s; }
};

BertClass.prototype.encode_float = function (Obj) {
    var s = Obj.toExponential();
    while (s.length < 31) { s += this.ZERO; }
    return this.FLOAT + s;
};

BertClass.prototype.encode_object = function (Obj) {
    if (Obj.type === "Atom") { return this.encode_atom(Obj); }
    if (Obj.type === "Binary") { return this.encode_binary(Obj); }
    if (Obj.type === "Tuple") { return this.encode_tuple(Obj); }
    if (Obj.constructor.toString().indexOf("Array") !== -1) { return this.encode_array(Obj); }
    return this.encode_associative_array(Obj);
};

BertClass.prototype.encode_atom = function (Obj) { return this.ATOM + this.int_to_bytes(Obj.value.length, 2) + Obj.value; };
BertClass.prototype.encode_binary = function (Obj) { return this.BINARY + this.int_to_bytes(Obj.value.length, 4) + Obj.value; };
BertClass.prototype.encode_tuple = function (Obj) {
    var i, s = "";
    if (Obj.length < 256) { s += this.SMALL_TUPLE + this.int_to_bytes(Obj.length, 1); }
    else { s += this.LARGE_TUPLE + this.int_to_bytes(Obj.length, 4); }
    for (i = 0; i < Obj.length; i++) { s += this.encode_inner(Obj[i]); }
    return s;
};

BertClass.prototype.encode_array = function (Obj) {
    var i, s = this.LIST + this.int_to_bytes(Obj.length, 4);
    for (i = 0; i < Obj.length; i++) { s += this.encode_inner(Obj[i]); }
    s += this.NIL;
    return s;
};

BertClass.prototype.encode_associative_array = function (Obj) {
    var key, Arr = [];
    for (key in Obj) { if (Obj.hasOwnProperty(key)) { Arr.push(this.tuple(this.atom(key), Obj[key])); } }
    return this.encode_array(Arr);
};

// - DECODING -

BertClass.prototype.decode_inner = function (S) {
    var Type = S[0];
    S = S.substring(1);
    switch (Type) {
        case this.SMALL_ATOM: return this.decode_atom(S, 1);
        case this.ATOM: return this.decode_atom(S, 2);
        case this.BINARY: return this.decode_binary(S);
        case this.SMALL_INTEGER: return this.decode_integer(S, 1);
        case this.INTEGER: return this.decode_integer(S, 4);
        case this.SMALL_BIG: return this.decode_big(S, 1);
        case this.LARGE_BIG: return this.decode_big(S, 4);
        case this.FLOAT: return this.decode_float(S);
        case this.STRING: return this.decode_string(S);
        case this.LIST: return this.decode_list(S);
        case this.SMALL_TUPLE: return this.decode_tuple(S, 1);
        case this.LARGE_TUPLE: return this.decode_large_tuple(S, 4);
        case this.NIL: return this.decode_nil(S);
        default: throw ("Unexpected BERT type: " + S.charCodeAt(0));
    }
};

BertClass.prototype.decode_atom = function (S, Count) {
    var Size, Value;
    Size = this.bytes_to_int(S, Count);
    S = S.substring(Count);
    Value = S.substring(0, Size);
    if (Value === "true") { Value = true; }
    else if (Value === "false") { Value = false; }
    return { value: this.atom(Value), rest:  S.substring(Size) };
};

BertClass.prototype.decode_binary = function (S) {
    var Size = this.bytes_to_int(S, 4);
    S = S.substring(4);
    return { value: this.binary(S.substring(0, Size)), rest:  S.substring(Size) };
};

BertClass.prototype.decode_integer = function (S, Count) {
    var Value = this.bytes_to_int(S, Count);
    S = S.substring(Count);
    return { value: Value, rest: S };
};

BertClass.prototype.decode_big = function (S, Count) {
    var Size, Value;
    Size = this.bytes_to_int(S, Count);
    S = S.substring(Count);
    Value = this.bytes_to_bignum(S, Size);
    return { value : Value, rest: S.substring(Size + 1) };
};

BertClass.prototype.decode_float = function (S) {
    var Size = 31;
    return { value: parseFloat(S.substring(0, Size)), rest: S.substring(Size) };
};

BertClass.prototype.decode_string = function (S) {
    var Size = this.bytes_to_int(S, 2);
    S = S.substring(2);
    return { value: S.substring(0, Size), rest:  S.substring(Size) };
};

BertClass.prototype.decode_list = function (S) {
    var Size, i, El, LastChar, Arr = [];
    Size = this.bytes_to_int(S, 4);
    S = S.substring(4);
    for (i = 0; i < Size; i++) { El = this.decode_inner(S); Arr.push(El.value); S = El.rest; }
    LastChar = S[0];
    if (LastChar !== this.NIL) { throw ("List does not end with NIL!"); }
    S = S.substring(1);
    return { value: Arr, rest: S };
};

BertClass.prototype.decode_tuple = function (S, Count) {
    var Size, i, El, Arr = [];
    Size = this.bytes_to_int(S, Count);
    S = S.substring(Count);
    for (i = 0; i < Size; i++) { El = this.decode_inner(S); Arr.push(El.value); S = El.rest; }
    return { value: this.tuple(Arr), rest: S };
};

BertClass.prototype.decode_nil = function (S) { return { value: [], rest: S }; };

// - UTILITY FUNCTIONS -

BertClass.prototype.int_to_bytes = function (Int, Length) {
    var isNegative, OriginalInt, i, Rem, s = "";
    isNegative = (Int < 0);
    if (isNegative) { Int = Int * (0 - 1); }
    OriginalInt = Int;
    for (i = 0; i < Length; i++) { 
        Rem = Int % 256;
        if (isNegative) { Rem = 255 - Rem; }
        s = String.fromCharCode(Rem) + s;
        Int = Math.floor(Int / 256);
    }
    if (Int > 0) { throw ("Argument out of range: " + OriginalInt); }
    return s;
};

BertClass.prototype.bytes_to_int = function (S, Length) {
    var isNegative, i, n, Num = 0;
    isNegative = (S.charCodeAt(0) > 128);
    for (i = 0; i < Length; i++) {
        n = S.charCodeAt(i);
        if (isNegative) { n = 255 - n; }
        if (Num === 0) { Num = n; }
        else { Num = Num * 256 + n; }
    }
    if (isNegative) { Num = Num * (0 - 1); }
    return Num;
};

BertClass.prototype.bignum_to_bytes = function (Int) {
    var isNegative, Rem, s = "";
    isNegative = Int < 0;
    if (isNegative) { Int *= -1; s += String.fromCharCode(1); } else { s += String.fromCharCode(0); }
    while (Int !== 0) { Rem = Int % 256; s += String.fromCharCode(Rem); Int = Math.floor(Int / 256); }
    return s;
};

BertClass.prototype.bytes_to_bignum = function (S, Count) {
    var isNegative, i, n, Num = 0;
    isNegative = (S.charCodeAt(0) === 1);
    S = S.substring(1);
    for (i = Count - 1; i >= 0; i--) {
        n = S.charCodeAt(i);
        if (Num === 0) { Num = n; } else { Num = Num * 256 + n; }
    }
    if (isNegative) { return Num * -1; }
    return Num;
};

BertClass.prototype.bytes_to_string = function (byteArray) {
    var i, s = "";
    for (i = 0; i < byteArray.length; i++) {
        s += byteArray[i] === 0x25 ? "%25" :
        (i > 3 ? 
            (byteArray[i] <= 0x7F ? String.fromCharCode(byteArray[i]) : "%" + byteArray[i].toString(16).toUpperCase())
            : String.fromCharCode(byteArray[i]) 
        );
    }
    return decodeURIComponent(s);
};

var Bert = new BertClass();
function bullet(url) {
	var CONNECTING = 0;
	var OPEN = 1;
	var CLOSING = 2;
	var CLOSED = 3;

	var transports = {
		/**
			The websocket transport is disabled for Firefox 6.0 because it
			causes a crash to happen when the connection is closed.
			@see https://bugzilla.mozilla.org/show_bug.cgi?id=662554
		*/
		websocket: function(){
			var transport = null;

			if (window.WebSocket){
				transport = window.WebSocket;
			}

			if (window.MozWebSocket
					&& navigator.userAgent.indexOf("Firefox/6.0") == -1){
				transport = window.MozWebSocket;
			}

                       transport.binaryType = "arrayuffer";

			if (transport){
				return {'heart': true, 'transport': transport};
			}

			return null;
		},

		xhrPolling: function(){
			var timeout;
			var xhr;

			var fake = {
				readyState: CONNECTING,
				send: function(data){
					if (this.readyState != CONNECTING && this.readyState != OPEN){
						return false;
					}

					var fakeurl = url.replace('ws:', 'http:').replace('wss:', 'https:');

					$.ajax({
						async: false,
						cache: false,
						type: 'POST',
						url: fakeurl,
						data: data,
						dataType: 'text',
						contentType:
							'application/x-www-form-urlencoded; charset=utf-8',
						headers: {'X-Socket-Transport': 'xhrPolling'},
						success: function(data){
							if (data.length != 0){
								fake.onmessage({'data': data});
							}
						}
					});

					return true;
				},
				close: function(){
					this.readyState = CLOSED;
					xhr.abort();
					clearTimeout(timeout);
					fake.onclose();
				},
				onopen: function(){},
				onmessage: function(){},
				onerror: function(){},
				onclose: function(){}
			};

			function poll(){
				var fakeurl = url.replace('ws:', 'http:').replace('wss:', 'https:');

				xhr = $.ajax({
					type: 'GET',
					cache: false,
					url: fakeurl,
					dataType: 'text',
					data: {},
					headers: {'X-Socket-Transport': 'xhrPolling'},
					success: function(data){
						if (fake.readyState == CONNECTING){
							fake.readyState = OPEN;
							fake.onopen(fake);
						}
						// Connection might have closed without a response body
						if (data.length != 0){
							fake.onmessage({'data': data});
						}
						if (fake.readyState == OPEN){
							nextPoll();
						}
					},
					error: function(xhr){
						fake.onerror();
					}
				});
			}

			function nextPoll(){
				timeout = setTimeout(function(){poll();}, 100);
			}

			nextPoll();

			return {'heart': false, 'transport': function(){ return fake; }};
		}
	};

	var tn = 0;
	function next(){
		var c = 0;

		for (var f in transports){
			if (tn == c){
				var t = transports[f]();
				if (t){
					var ret = new t.transport(url);
					ret.heart = t.heart;
					return ret;
				}

				tn++;
			}

			c++;
		}

		return false;
	}

	var stream = new function(){
		var isClosed = true;
		var readyState = CLOSED;
		var heartbeat;
		var delay = 80;
		var delayDefault = 80;
		var delayMax = 10000;

		var transport;
		function init(){

			isClosed = false;
			readyState = CONNECTING;
			transport = next();

			if (!transport){
				// Hard disconnect, inform the user and retry later
				delay = delayDefault;
				tn = 0;
				stream.ondisconnect();
				setTimeout(function(){init();}, delayMax);
				return false;
			}

			transport.onopen = function(){
				// We got a connection, reset the poll delay
				delay = delayDefault;

				if (transport.heart){
					heartbeat = setInterval(function(){stream.onheartbeat();}, 20000);
				}

				if (readyState != OPEN){
					readyState = OPEN;
					stream.onopen();
				}
			};
			transport.onclose = function(){
				// Firefox 13.0.1 sends 2 close events.
				// Return directly if we already handled it.
				if (isClosed){
					return;
				}

				transport = null;
				clearInterval(heartbeat);

				if (readyState == CLOSING){
					readyState = CLOSED;
					stream.onclose();
				} else{
					// Close happened on connect, select next transport
					if (readyState == CONNECTING){
						tn++;
					}

					delay *= 2;
					if (delay > delayMax){
						delay = delayMax;
					}

					isClosed = true;

					setTimeout(function(){
						init();
					}, delay);
				}
			};
			transport.onerror = transport.onclose;
			transport.onmessage = function(e){
				stream.onmessage(e);
			};
		}
		init();

		this.onopen = function(){};
		this.oninit = function(){};
		this.onmessage = function(){};
		this.ondisconnect = function(){};
		this.onclose = function(){};
		this.onheartbeat = function(){};

		this.setURL = function(newURL){
			url = newURL;
		};
		this.send = function(data){
			if (transport){
				return transport.send(data);
			} else{
				return false;
			}
		};
		this.close = function(){
			readyState = CLOSING;
			if (transport){
				transport.close();
			}
		};
	};

	return stream;
}
var msg = 0;
var ws;
var utf8 = {};

function addStatus(text){
    var date = new Date();
    document.getElementById('n2ostatus').innerHTML =
    document.getElementById('n2ostatus').innerHTML + "E> " + text + "<br/>";
}

utf8.toByteArray = function(str) {
    var byteArray = [];
    for (var i = 0; i < str.length; i++)
        if (str.charCodeAt(i) <= 0x7F)
            byteArray.push(str.charCodeAt(i));
        else {
            var h = encodeURIComponent(str.charAt(i)).substr(1).split('%');
            for (var j = 0; j < h.length; j++)
                byteArray.push(parseInt(h[j], 16));
        }
    return byteArray;
};

function WebSocketsInit(){
//    if ("MozWebSocket" in window) { WebSocket = MozWebSocket; }
//    if ("WebSocket" in window) {
        ws = new bullet("ws://"+window.location.hostname+
                            ":"+window.location.port+
                   "/websocket"+window.location.pathname+
                                window.location.search);
        initialized = false;
        ws.onopen = function() { if (!initialized) { ws.send(['N2O',TransitionProcess]); initialized = true; } };
        ws.onmessage = function (evt) {
            msg = evt.data;
            var actions = msg;//Bert.decodebuf(msg);;
            addStatus("Received: '" + actions + "'");
            eval(actions);
        };
        ws.onclose = function() { addStatus("websocket was closed"); };
//    } else {
//        addStatus("sorry, your browser does not support websockets.");
//    }
}

WebSocketsInit();
