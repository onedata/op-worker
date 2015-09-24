App.FileListController = Ember.ArrayController.extend({
    testVal: 'test val',

    allAreSelected: function(key, value) {
        if (value === undefined) {
            return !!this.get('length') && this.isEvery('selected', true);
        } else {
            this.setEach('selected', value);
            this.invoke('save');
            return value;
        }
    }.property('@each.selected'),

    isAnySelected: function(key, value) {
        if (value === undefined) {
            return !!this.get('length') && this.isAny('selected', true);
        } else {
            return value;
        }
    }.property('@each.selected'),

    inNoneSelected: Ember.computed.not('isAnySelected'),
    
    actions: {
        createNew: function () {
            var name = this.get('newFileName');
            var attr = this.get('newFileAttr');
            if (!name.trim())  return;
            if (!attr.trim())  return;

            var file = this.store.createRecord('file', {
                name: name,
                attribute: attr,
                selected: false
            });

            file.save();
        }
    }
});