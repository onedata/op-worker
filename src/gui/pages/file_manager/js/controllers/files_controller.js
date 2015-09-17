FileManager.MainController = Ember.ArrayController.extend({
    allAreSelected: function(key, value) {
        if (value === undefined) {
            return !!this.get('length') && this.isEvery('selected', true);
        } else {
            this.setEach('selected', value);
            this.invoke('save');
            return value;
        }
    }.property('@each.selected')
});