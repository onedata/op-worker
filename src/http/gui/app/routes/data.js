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
    return [
      {
        id: 1,
        name: 'space 1',
        isDefault: false,
      },
      {
        id: 2,
        name: 'space 2',
        isDefault: true,
      },
      {
        id: 3,
        name: 'space 3',
        isDefault: true,
      }
    ];
  },

  actions: {
    goToDataSpace(spaceId) {
      this.transitionTo('data.data-space', spaceId);
    }
  }
});
