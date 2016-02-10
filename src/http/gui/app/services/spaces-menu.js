/**
 * Allows manipulation of spaces-menu look.
 * Delegates actions to spaces-menu component.
 *
 * Delegated API methods:
 * - selectSpace(space)
 * - clearSpaceSelection
 * - selectSubmenu(optionName)
 *
 * @see {@link components/main-menu} for the API implementation
 * @module services/main-menu
 * @author Jakub Liput
 * @copyright (C) 2016 ACK CYFRONET AGH
 * @license This software is released under the MIT license cited in 'LICENSE.txt'.
 */

import Ember from 'ember';

export default Ember.Service.extend(Ember.Evented);
