import DS from 'ember-data';

export default DS.Model.extend({
  name: DS.attr('string'),
  type: DS.attr('string'),
  content: DS.belongsTo('fileContent', {async: true}),
  parentId: DS.attr('string'),
  parent: DS.belongsTo('file', {inverse: 'children', async: true}),
  children: DS.hasMany('file', {inverse: 'parent', async: true}),

  expanded: false,
  selected: false,

  isDir: function () {
    return this.get('type') === 'dir';
  }.property('type'),


  isVisible: function () {
    var visible = this.get('parent.expanded');
    console.log('deselect(' + this.get('name') + '): ' +
      (this.get('selected') && !visible));
    if (this.get('selected') && !visible) {
      this.set('selected', false);
    }
    return visible;
  }.property('parent.expanded')
});
