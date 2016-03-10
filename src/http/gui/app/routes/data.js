/**
 * Lists a Spaces whose allows to browse files in sub-routes.
 *
 * @module routes/data
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Route.extend({
  model() {
    return this.store.findAll('data-space');
  },

  actions: {
    goToDataSpace(spaceId) {
      this.transitionTo('data.data-space', spaceId);
    }
  }
});
