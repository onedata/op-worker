/**
 * This is a prototype model representing a file in file browser.
 * New implementation with data-space support.
 * @module models/file
 * @author Łukasz Opioła
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import DS from 'ember-data';

export default DS.Model.extend({
  name: DS.attr('string'),
  /**
    Specifies is this object a regular file ("file") or directory ("dir")
    To check if it is a dir please use "isDir" property.
  */
  type: DS.attr('string'),
  parent: DS.belongsTo('file', {inverse: 'children', async: true}),
  children: DS.hasMany('file', {inverse: 'parent', async: true}),

  // TODO: this information will be probably stored in component
  isExpanded: true,
  isSelected: false,

  isDir: function () {
    return this.get('type') === 'dir';
  }.property('type'),

  isVisible: function () {
    var visible = this.get('parent.isExpanded');
    console.log('deselect(' + this.get('name') + '): ' +
      (this.get('isSelected') && !visible));
    if (this.get('isSelected') && !visible) {
      this.set('isSelected', false);
    }
    return visible;
  }.property('parent.isExpanded')
});
