App.File = DS.Model.extend({
    name: DS.attr('string'),
    parentId: DS.attr('string'),
    parent: DS.belongsTo('file', {inverse: 'children', async: true}),
    children: DS.hasMany('file', {inverse: 'parent', async: true})
    //selected: DS.attr('boolean')
});
