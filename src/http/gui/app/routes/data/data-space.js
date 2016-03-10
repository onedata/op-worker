import Ember from 'ember';

/**
 * Load model for space - to be able to browse it's root dir.
 *
 * @module routes/data
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default Ember.Route.extend({
  model(params) {
    return this.store.findRecord('data-space', params.data_space_id);
  },

  activate() {
    Ember.run.scheduleOnce('afterRender', this, function() {
      console.debug('selected data-space: ' + this.model.get('id'));
      // TODO: this should use data-spaces-select service or something...

    });
  },

  renderTemplate() {
    this.render({outlet: 'data-space'});
  },

  actions: {
    showDirInBrowser(fileId) {
      this.transitionTo('data.data-space.dir', fileId);
    }
  }
});
