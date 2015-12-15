App.File = DS.Model.extend({

    name: DS.attr('string'),
    type: DS.attr('string'),
    parentId: DS.attr('string'),
    parent: DS.belongsTo('file', {inverse: 'children', async: true}),
    children: DS.hasMany('file', {inverse: 'parent', async: true}),

    selected: false,
    expanded: false,

    depth: 0,
    calculateDepth: function () {
        if (this.get('parentId') == 'root') {
            this.set('depth', 0);
        } else {
            this.set('depth', this.get('parent').get('depth') + 1);
        }
    }.observes('parent.depth'),

    isDir: function () {
        return this.get('type') == 'dir';
    }.property('type')
});
