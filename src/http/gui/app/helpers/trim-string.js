import Ember from 'ember';

export function trimString(params/*, hash*/) {
  let text = params[0];
  if (text) {
    let length = params[1] || 15;

    let addEllipsis = (text.length > length);

    return addEllipsis ? (text.substring(0, length-3) + '...') : text;
  } else {
    return text;
  }
}

export default Ember.Helper.helper(trimString);
