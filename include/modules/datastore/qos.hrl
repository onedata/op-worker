%%%-------------------------------------------------------------------
%%% @author Michal Cwiertnia
%%% @copyright (C) 2019 ACK CYFRONET AGH
%%% This software is released under the MIT license
%%% cited in 'LICENSE.txt'.
%%%--------------------------------------------------------------------
%%% @doc
%%% This file contains definitions of macros used by qos module.
%%%
%%% QoS management is based on two types of documents synchronized within space:
%%% qos_entry and file_qos.
%%%
%%% The qos_entry document contains information about single QoS requirement
%%% including QoS expression, number of required replicas, fulfillment status
%%% and UUID of file or directory for which requirement has been added.
%%% Such document is created when user adds QoS requirement for file or directory.
%%% Requirement added for directory is inherited by whole directory structure.
%%% Each QoS requirement is evaluated separately. It means that it is not
%%% possible to define inconsistent requirements. For example if one requirement
%%% says that file should be present on storage in Poland and other requirement
%%% says that file should be present on storage in any country but Poland,
%%% two different replicas will be created. On the other hand the same file
%%% replica can fulfill multiple different QoS requirements. For example if
%%% there is storage of type disk in Poland, then replica on such storage can
%%% fulfill requirements that demands replica on storage in Poland and requirements
%%% that demands replica on storage of type disk. System will create new file
%%% replica only if currently existing replicas don't fulfill QoS requirements.
%%% Multiple qos_entry documents can be created for the same file or directory.
%%% Adding two identical QoS requirements for the same file results in two
%%% different qos_entry documents. Each transfer scheduled to fulfill QoS
%%% is added to links tree.
%%% QoS requirement is considered as fulfilled when:
%%%     - all traverse tasks, triggered by creating this QoS requirement, are finished
%%%     - there are no remaining transfers, that were created to fulfill this QoS requirement
%%%
%%% The file_qos item contains aggregated information about QoS defined
%%% for file or directory. It contains:
%%%     - qos_entries - holds IDs of all qos_entries defined for this file,
%%%     - target_storages - holds mapping storage_id to list of qos_entry IDs.
%%%       When new QoS is added for file or directory, storages on which replicas
%%%       should be stored are calculated using QoS expression. Then this mapping
%%%       is appropriately updated with the calculated storages.
%%% Each file or directory can be associated with at most one such document.
%%% It is created/updated when new qos_entry document is created for file or
%%% directory. In this case target storages are chosen according to QoS expression
%%% and number of required replicas defined in qos_entry document.
%%% Then file_qos document is updated - qos_entry ID is added to QoS entries list
%%% and target storages mapping.
%%% According to this getting full information about QoS defined for file or
%%% directory requires calculating effective file_qos as file_qos document
%%% is not created for each file separately.
%%%
%%% NOTE!!!
%%% If you introduce any changes in this module, please ensure that
%%% docs in file_qos.erl and qos_entry.erl are up to date.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(QOS_HRL).
-define(QOS_HRL, 1).

% macros used for operations on QoS expression
-define(UNION, <<"|">>).
-define(INTERSECTION, <<"&">>).
% TODO: VFS-5712 for now used "-" instead of "\" as backslash is an escape character,
% have to discuss that
-define(COMPLEMENT, <<"-">>).
-define(OPERATORS, [?UNION, ?INTERSECTION, ?COMPLEMENT]).

-define(L_PAREN, <<"(">>).
-define(R_PAREN, <<")">>).
-define(EQUALITY, <<"=">>).


% macros used for operations on QoS bounded cache
-define(CACHE_TABLE_NAME(SpaceId), binary_to_atom(SpaceId, utf8)).
-define(QOS_BOUNDED_CACHE_GROUP, <<"qos_bonded_cache_group">>).


-define(IMPOSSIBLE_QOS_KEY, <<"impossible_qos_key">>).

-endif.
