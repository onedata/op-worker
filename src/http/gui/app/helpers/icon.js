import Ember from 'ember';

/**
 * Insert an icon from onedata icons collection.
 *
 * @param {string} name - icon name in collection (see icon-* style classes)
 * @param {string} classes - additional classes, eg. for color (see color-* style classes)
 *
 * @returns {string} an icon span
 */
export default Ember.Helper.helper(function(params) {
  let name = params[0];
  let classes = params[1] || '';

  var html = '<span class="oneicon-'+name+' '+classes+'"></span>';
  return new Ember.Handlebars.SafeString(html);
});
