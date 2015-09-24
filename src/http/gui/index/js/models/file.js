App.File = DS.Model.extend({
    name: DS.attr('string'),
    attribute: DS.attr(),
    selected: DS.attr('boolean')
});
