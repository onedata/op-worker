import Ember from 'ember';

/**
 * Single space Route - loads Space data before actions/resources for a single
 * space.
 * @module routes/spaces/show
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */
export default Ember.Route.extend({
  spacesMenuService: Ember.inject.service('spaces-menu'),

  model(params) {
    return this.store.find('space', params.space_id);
  },

  afterModel(selectedSpace) {
    console.debug(`space show afterModel: ${selectedSpace} id ${selectedSpace.get('id')}`);
    Ember.run.scheduleOnce('afterRender', this, function() {
      console.debug(`will trigger select space: ${selectedSpace} id ${selectedSpace.get('id')}`);
      this.get('spacesMenuService').trigger('selectSpace', selectedSpace);
      $('nav.secondary-sidebar').addClass('visible');
    });

    // by default, open users settings
    this.transitionTo('spaces.show.users');
  }
});
