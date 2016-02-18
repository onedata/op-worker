/**
 * Allows manipulation of main-menu.
 * Delegates actions to main-menu component.
 *
 * Delegated API methods:
 * - selectItem(itemName)
 * - deselectItem(itemName)
 * - clearSelection
 *
 * @see {@link components/main-menu} for the API implementation
 * @module services/main-menu
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Service.extend(Ember.Evented);
