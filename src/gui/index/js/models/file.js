App.File = DS.Model.extend({
    name: DS.attr('string'),
    attribute: DS.attr(),
    selected: DS.attr('boolean')
});

App.File.FIXTURES = [
    {
        id: 1,
        name: 'File 1',
        attribute: 34234,
        selected: false
    },
    {
        id: 2,
        name: 'File 234',
        attribute: 123123555,
        selected: false
    }
];
