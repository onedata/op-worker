/**
 * Insert a onedata checkbox icon representing four states: true/false unmodified/modified.
 * Eg. a modified false can be a green "x" in circle (depends on styles).
 *
 * @param {Boolean} value - true/false state as in checkbox (checked/unchecked)
 * @param {Boolean} isModified - true indicates that the value of checkbox
 *   was modified without saving
 * @returns {string} an icon span
 *
 * @module helpers/bool-option
 * @todo maybe it should be a Component
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
*/

import Ember from 'ember';

export default Ember.Helper.helper(function(params) {
  let value = params[0];
  let isModified = params[1];

  var cssClasses = (value ? 'oneicon-checkbox-check' : 'oneicon-checkbox-x');
  if (isModified === true) {
    cssClasses += ' modified';
  }

  // TODO: use icon helper
  var html = '<span class="one-checkbox '+cssClasses+'" aria-hidden="true"></span>';
  return new Ember.Handlebars.SafeString(html);
});
