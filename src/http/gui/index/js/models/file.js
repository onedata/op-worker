App.File = DS.Model.extend({

    name: DS.attr('string'),
    type: DS.attr('string'),
    content: DS.belongsTo('fileContent', {async: true}),
    parentId: DS.attr('string'),
    parent: DS.belongsTo('file', {inverse: 'children', async: true}),
    children: DS.hasMany('file', {inverse: 'parent', async: true}),

    expanded: false,
    selected: false,

    isDir: function () {
        return this.get('type') == 'dir';
    }.property('type'),


    isVisible: function () {
        return this.get('parent.isExpanded');
    }.property('parent.isExpanded')
});

App.FileContent = DS.Model.extend({
    bytes: DS.attr('string'),
    file: DS.belongsTo('file')
});