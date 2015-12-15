App.SortedFileChildrenController = Ember.ArrayController.extend({
    sortProperties: ['type:asc', 'name:asc'],
    sortedModel: Ember.computed.sort("model", "sortProperties")
});