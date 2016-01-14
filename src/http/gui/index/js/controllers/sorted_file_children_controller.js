// This is a prototype controller used for sorting file list by type and name.

App.SortedFileChildrenController = Ember.ArrayController.extend({
    sortProperties: ['type:asc', 'name:asc'],
    sortedModel: Ember.computed.sort("model", "sortProperties")
});