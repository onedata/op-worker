import DS from 'ember-data';

/** A configuration of a space - entry point for all options
 *  that can be reached from "spaces" button in primary sidebar.
 *
 *  TODO: don't know if should list attributes here, maybe it's redundant
 */
export default DS.Model.extend({
  /** User specified name of space that will be exposed in GUI */
  name: DS.attr('string'),
  /** Collection of users permissions - effectively all rows in permissions table */
  userPermissions: DS.hasMany('spaceUserPermission', {async: true}),
  /** Collection of group permissions - effectively all rows in permissions table */
  groupPermissions: DS.hasMany('spaceGroupPermission', {async: true}),
  /** Wether user specified this space as default */
  isDefault: DS.attr('boolean', {defaultValue: false}),

// TODO: currently not used - use list Order in templates
  /** An absolute position on list */
  listOrder: DS.attr('number'),

  /*** Non-presistable properties - probably shold be moved to components... ***/
  /** users, groups or permissions - option highlighted in Space submenu */
  currentMenuOption: null,

  /*** Template helper methods ***/

  // TODO: computing entryId in model is not very elegant
  /** Id of this space menu element used in sidebar HTML */
  sidebarEntryId: function() {
    return 'space-entry-'+this.id;
  }.property('id')
});
