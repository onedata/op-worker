import Ember from 'ember';

/**
 * Insert an icon from onedata icons collection.
 *
 * @param {string} name - icon name in collection (see icon-* style classes)
 * @param {string} classes - additional classes, eg. for color (see color-* style classes)
 *
 * @returns {string} an icon span
 * @module helpers/icon
 * @todo maybe it should be a Component
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/
export default Ember.Helper.helper(function(params) {
  let name = params[0];
  let classes = params[1] || '';

  var html = '<span class="oneicon oneicon-'+name+' '+classes+'"></span>';
  return new Ember.Handlebars.SafeString(html);
});
