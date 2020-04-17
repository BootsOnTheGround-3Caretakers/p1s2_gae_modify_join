from __future__ import absolute_import
from __future__ import unicode_literals

import os
import sys

import webapp2
from google.appengine.ext import ndb

cwd = os.getcwd()
sys.path.insert(0, 'includes')
from datavalidation import DataValidation
from GCP_return_codes import FunctionReturnCodes as RC
from task_queue_functions import TaskQueueFunctions
from p1_services import Services, TaskArguments
from p1_global_settings import PostDataRules
from p1_datastores import Datastores
from datastore_functions import DatastoreFunctions as DSF
from error_handling import RDK


class CommonPostHandler(DataValidation):
    def post(self):
        task_id = "modify-joins:CommonPostHandler:post"
        debug_data = []
        call_result = self.processPushTask()
        debug_data.append(call_result)
        task_results = call_result['task_results']

        params = {}
        for key in self.request.arguments():
            params[key] = self.request.get(key, None)
        task_functions = TaskQueueFunctions()

        if call_result[RDK.success] != RC.success:
            task_functions.logError(
                call_result[RDK.success], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result[RDK.return_msg], debug_data,
                self.request.get('transaction_user_uid', None)
            )
            task_functions.logTransactionFailed(self.request.get('transaction_id', None), call_result[RDK.success])
            if call_result[RDK.success] < RC.retry_threshold:
                self.response.set_status(500)
            else:
                #any other failure scenario will continue to fail no matter how many times its called.
                self.response.set_status(200)
            return

        # go to the next function
        task_functions = TaskQueueFunctions()
        call_result = task_functions.nextTask(task_id, task_results, params)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            task_functions.logError(
                call_result[RDK.success], task_id, params,
                self.request.get('X-AppEngine-TaskName', None),
                self.request.get('transaction_id', None), call_result[RDK.return_msg], debug_data,
                self.request.get('transaction_user_uid', None)
            )
        # </end> go to the next function
        self.response.set_status(200)


class AddModifyClusterUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AddModifyClusterUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        cluster_uid = unicode(self.request.get(TaskArguments.s2t1_cluster_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t1_user_uid, ""))
        user_roles = unicode(self.request.get(TaskArguments.s2t1_user_roles, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [cluster_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [user_roles, PostDataRules.required_name],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        cluster_uid = long(cluster_uid)
        user_uid = long(user_uid)
        user_key = ndb.Key(Datastores.users._get_kind(), user_uid)

        existings_keys = [
            ndb.Key(Datastores.cluster._get_kind(), cluster_uid),
            user_key,
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        parent_key = ndb.Key(Datastores.cluster._get_kind(), cluster_uid)
        key_name = "{}|{}".format(user_uid, cluster_uid)
        joins = Datastores.cluster_joins(id=key_name, parent=parent_key)
        joins.user_uid = user_uid
        joins.cluster_uid = cluster_uid
        joins.roles = user_roles
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to write cluster_joins to datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        cluster_pointer = Datastores.cluster_pointer(parent=user_key)
        cluster_pointer.cluster_uid = cluster_uid
        call_result = cluster_pointer.kput()
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to write cluster_pointer to datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class RemoveUserFromCluster(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveUserFromCluster:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        cluster_uid = unicode(self.request.get(TaskArguments.s2t2_cluster_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t2_user_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [cluster_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        cluster_uid = long(cluster_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.cluster._get_kind(), cluster_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.cluster._get_kind(), cluster_uid,
            Datastores.cluster_joins._get_kind(), "{}|{}".format(user_uid, cluster_uid)
        )

        call_result = DSF.kget(key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to load cluster_joins from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }
        cluster_join = call_result['get_result']
        if not cluster_join:
            return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}

        cluster_join.replicateEntityToFirebase(delete_flag=True)
        if call_result[RDK.success] != RC.success:
            return_msg += "firebase deletion replication failed"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to delete cluster_joins from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class AddModifyUserSkill(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AddModifyUserSkill:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t3_user_uid, ""))
        skill_uid = unicode(self.request.get(TaskArguments.s2t3_skill_uid, ""))
        special_notes = unicode(self.request.get(TaskArguments.s2t3_special_notes, "")) or None
        total_capacity = unicode(self.request.get(TaskArguments.s2t3_total_capacity))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [skill_uid, PostDataRules.internal_uid],
            [special_notes, Datastores.caretaker_skills_joins._rule_special_notes],
            [total_capacity, PostDataRules.positive_number],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        user_uid = long(user_uid)
        skill_uid = long(skill_uid)
        total_capacity = int(total_capacity)
        user_key = ndb.Key(Datastores.users._get_kind(), user_uid)
        skill_key = ndb.Key(Datastores.caretaker_skills._get_kind(), skill_uid)

        existings_keys = [
            user_key,
            skill_key,
        ]

        existing_entities = []
        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            entity = call_result['get_result']
            if not entity:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            existing_entities.append(entity)

        user = existing_entities[0]
        if not (user.country_uid and user.region_uid and user.area_uid):
            return_msg += "country_uid, region_uid, or area_uid of the user not set yet."
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }
        # </end> verify input data

        key_name = "{}|{}".format(user_uid, skill_uid)
        joins = Datastores.caretaker_skills_joins(id=key_name, parent=user_key)
        joins.user_uid = user_uid
        joins.skill_uid = skill_uid
        joins.special_notes = special_notes
        joins.total_capacity = total_capacity
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to write caretaker_skills_joins to datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class AddModifyNeedToNeeder(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AddModifyNeedToNeeder:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        need_uid = unicode(self.request.get(TaskArguments.s2t4_need_uid, ""))
        needer_uid = unicode(self.request.get(TaskArguments.s2t4_needer_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t4_user_uid, ""))
        special_requirements = unicode(self.request.get(TaskArguments.s2t4_special_requests, "")) or None

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [need_uid, PostDataRules.internal_uid],
            [needer_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [special_requirements, Datastores.needer_needs_joins._rule_special_requests],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        needer_uid = long(needer_uid)
        need_uid = long(need_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid),
            ndb.Key(Datastores.needs._get_kind(), need_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        parent_key = ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid)
        key_name = "{}|{}".format(needer_uid, need_uid)
        joins = Datastores.needer_needs_joins(id=key_name, parent=parent_key)
        joins.needer_uid = needer_uid
        joins.need_uid = need_uid
        joins.user_uid = user_uid
        joins.special_requests = special_requirements
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to write needer_need_joins to datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class RemoveNeedFromNeeder(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveNeedFromNeeder:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        need_uid = unicode(self.request.get(TaskArguments.s2t5_need_uid, ""))
        needer_uid = unicode(self.request.get(TaskArguments.s2t5_needer_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t5_user_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [need_uid, PostDataRules.internal_uid],
            [needer_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        needer_uid = long(needer_uid)
        need_uid = long(need_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid),
            ndb.Key(Datastores.needs._get_kind(), need_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.users._get_kind(), user_uid,
            Datastores.needer._get_kind(), needer_uid,
            Datastores.needer_needs_joins._get_kind(), "{}|{}".format(needer_uid, need_uid)
        )

        call_result = DSF.kget(key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to get needer_need_joins from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }
        entity = call_result['get_result']
        if not entity:
            return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}

        entity.replicateEntityToFirebase(delete_flag=True)
        if call_result[RDK.success] != RC.success:
            return_msg += "firebase deletion replication failed"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to delete needer_need_joins from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class RemoveNeederFromUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveNeederFromUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        needer_uid = unicode(self.request.get(TaskArguments.s2t6_needer_uid, ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t6_user_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [needer_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        needer_uid = long(needer_uid)
        user_uid = long(user_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.needer._get_kind(), needer_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.users._get_kind(), user_uid,
            Datastores.needer._get_kind(), needer_uid
        )

        call_result = DSF.kget(key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to load needer from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }
        entity = call_result['get_result']
        if not entity:
            return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}

        entity.replicateEntityToFirebase(delete_flag=True)
        if call_result[RDK.success] != RC.success:
            return_msg += "firebase deletion replication failed"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to delete needer from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class AssignHashtagToUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AssignHashtagToUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t7_user_uid, ""))
        hashtag_uid = unicode(self.request.get(TaskArguments.s2t7_hashtag_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [hashtag_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        user_uid = long(user_uid)
        hashtag_uid = long(hashtag_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.hashtags._get_kind(), hashtag_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        parent_key = ndb.Key(Datastores.users._get_kind(), user_uid)
        pointer = Datastores.hashtag_pointer(id=hashtag_uid, parent=parent_key)
        pointer.hashtag_uid = hashtag_uid
        call_result = pointer.kput()
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to write hashtag_pointer to datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class RemoveHashtagFromUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveHashtagFromUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t8_user_uid, ""))
        hashtag_uid = unicode(self.request.get(TaskArguments.s2t8_hashtag_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [hashtag_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        user_uid = long(user_uid)
        hashtag_uid = long(hashtag_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.hashtags._get_kind(), hashtag_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(Datastores.users._get_kind(), user_uid, Datastores.hashtag_pointer._get_kind(), hashtag_uid)
        call_result = DSF.kget(key)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to load hashtag_pointer from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }
        entity = call_result['get_result']
        if not entity:
            return_msg += "hashtag_pointer not found"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }
        call_result = entity.replicateEntityToFirebase(delete_flag=True)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to delete hashtag_pointer on firebase"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to delete hashtag_pointer from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class RemoveSkillFromUser(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:RemoveSkillFromUser:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t9_user_uid, ""))
        skill_uid = unicode(self.request.get(TaskArguments.s2t9_skill_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [skill_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        user_uid = long(user_uid)
        skill_uid = long(skill_uid)

        existings_keys = [
            ndb.Key(Datastores.users._get_kind(), user_uid),
            ndb.Key(Datastores.caretaker_skills._get_kind(), skill_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        key = ndb.Key(
            Datastores.users._get_kind(), user_uid,
            Datastores.caretaker_skills_joins._get_kind(), "{}|{}".format(user_uid, skill_uid)
        )
        call_result = DSF.kdelete(transaction_user_uid, key)
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to delete caretaker_skills_joins from datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class ModifyUserInformation(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:ModifyUserInformation:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        user_uid = unicode(self.request.get(TaskArguments.s2t10_user_uid, ""))
        first_name = unicode(self.request.get(TaskArguments.s2t10_first_name, "")) or None
        last_name = unicode(self.request.get(TaskArguments.s2t10_last_name, "")) or None
        phone_number = unicode(self.request.get(TaskArguments.s2t10_phone_number, "")) or None
        phone_texts = unicode(self.request.get(TaskArguments.s2t10_phone_texts, "")) or None
        phone_2 = unicode(self.request.get(TaskArguments.s2t10_phone_2, "")) or None
        emergency_contact = unicode(self.request.get(TaskArguments.s2t10_emergency_contact, "")) or None
        home_address = unicode(self.request.get(TaskArguments.s2t10_home_address, "")) or None
        email_address = unicode(self.request.get(TaskArguments.s2t10_email_address, "")) or None
        firebase_uid = unicode(self.request.get(TaskArguments.s2t10_firebase_uid, "")) or None
        country_uid = unicode(self.request.get(TaskArguments.s2t10_country_uid, "")) or None
        region_uid = unicode(self.request.get(TaskArguments.s2t10_region_uid, "")) or None
        area_uid = unicode(self.request.get(TaskArguments.s2t10_area_uid, "")) or None
        description = unicode(self.request.get(TaskArguments.s2t10_description, "")) or None
        preferred_radius = unicode(self.request.get(TaskArguments.s2t10_preferred_radius, "")) or None
        account_flags = unicode(self.request.get(TaskArguments.s2t10_account_flags, "")) or None
        location_cord_lat = unicode(self.request.get(TaskArguments.s2t10_location_cord_lat, "")) or None
        location_cord_long = unicode(self.request.get(TaskArguments.s2t10_location_cord_long, "")) or None
        gender = unicode(self.request.get(TaskArguments.s2t10_gender, "")) or None

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [user_uid, PostDataRules.internal_uid],
            [first_name, PostDataRules.optional_name],
            [last_name, PostDataRules.optional_name],
            [phone_number, PostDataRules.optional_name],
            [phone_texts, Datastores.users._rule_phone_texts],
            [phone_2, PostDataRules.optional_name],
            [emergency_contact, Datastores.users._rule_emergency_contact],
            [home_address, Datastores.users._rule_home_address],
            [email_address, Datastores.users._rule_email_address],
            [firebase_uid, Datastores.users._rule_firebase_uid],
            [country_uid, Datastores.users._rule_country_uid],
            [region_uid, Datastores.users._rule_region_uid],
            [area_uid, Datastores.users._rule_area_uid],
            [description, Datastores.users._rule_description],
            [preferred_radius, PostDataRules.optional_number],
            [account_flags, Datastores.users._rule_account_flags],
            [location_cord_lat, PostDataRules.optional_name],
            [location_cord_long, PostDataRules.optional_name],
            [gender, PostDataRules.optional_name],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        transaction_user_uid = long(transaction_user_uid)
        user_uid = long(user_uid)
        preferred_radius = long(preferred_radius) if preferred_radius else None

        location_coord = None
        if location_cord_lat and location_cord_long:
            try:
                location_cord_lat = float(location_cord_lat)
            except ValueError as exc:
                return_msg += unicode(exc)
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            try:
                location_cord_long = float(location_cord_long)
            except ValueError as exc:
                return_msg += unicode(exc)
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not ((-90 <= location_cord_lat <= 90) and (-180 <= location_cord_long <= 180)):
                return_msg += "latitude value must be [-90, 90], longitude value must be [-180, 180]"
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            location_coord = ndb.GeoPt(location_cord_lat, location_cord_long)
        elif location_cord_lat or location_cord_long:
            return_msg += "Incomplete location information. latitude: {}, longitude: {}".format(location_cord_lat, location_cord_long)
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        user_key = ndb.Key(Datastores.users._get_kind(), user_uid)
        call_result = DSF.kget(user_key)
        if call_result[RDK.success] != RC.success:
            return_msg += "Failed to load user from datastore"
            return {
                RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        if (not (email_address and firebase_uid)) and (email_address or firebase_uid):
            return_msg += "Both email_address and firebase_uid must be specified when either one is specified."
            return {
                RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        user = call_result['get_result']
        if not user:
            return_msg += "User doesn't exist"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        if phone_number and user.phone_1 != phone_number:
            # check if there is another user having the same phone number
            key = ndb.Key(Datastores.phone_numbers._get_kind(), "{}|{}".format(country_uid, phone_number))
            call_result = DSF.kget(key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "failed to load phone_number from datastore"
                return {
                    RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            phone_number_entity = call_result['get_result']
            if phone_number_entity and phone_number_entity.user_uid != user_uid:
                return_msg += "The specified phone_number has been used by another user"
                return {
                    RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            #</end> check if there is another user having the same phone number

        # </end> verify input data

        user.first_name = first_name or user.first_name
        user.last_name = last_name or user.last_name
        user.phone_1 = phone_number or user.phone_1
        user.phone_texts = phone_texts or user.phone_texts
        user.phone_2 = phone_2 or user.phone_2
        user.emergency_contact = emergency_contact or user.emergency_contact
        user.home_address = home_address or user.home_address
        user.email_address = email_address or user.email_address
        user.firebase_uid = firebase_uid or user.firebase_uid
        user.country_uid = country_uid or user.country_uid
        user.region_uid = region_uid or user.region_uid
        user.area_uid = area_uid or user.area_uid
        user.description = description or user.description
        user.location_cords = location_coord or user.location_cords
        user.preferred_radius = preferred_radius or user.preferred_radius
        user.account_flags = account_flags or user.account_flags
        user.gender = gender or user.gender
        call_result = user.kput()
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to update user on datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        if phone_number:
            phone_number_entity = Datastores.phone_numbers(id="{}|{}".format(country_uid, phone_number))
            phone_number_entity.user_uid = user_uid
            call_result = phone_number_entity.kput()
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "failed to write phone_number to datastore"
                return {
                    RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results
                }

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


class AssociateSkillWithNeed(webapp2.RequestHandler, CommonPostHandler):
    def processPushTask(self):
        task_id = "modify-joins:AssociateSkillWithNeed:processPushTask"
        return_msg = task_id + ": "
        debug_data = []
        task_results = {}

        # verify input data
        transaction_id = unicode(self.request.get("transaction_id", ""))
        transaction_user_uid = unicode(self.request.get("transaction_user_uid", ""))
        skill_uid = unicode(self.request.get(TaskArguments.s2t11_skill_uid, ""))
        need_uid = unicode(self.request.get(TaskArguments.s2t11_need_uid, ""))

        call_result = self.ruleCheck([
            [transaction_id, PostDataRules.required_name],
            [transaction_user_uid, PostDataRules.internal_uid],
            [skill_uid, PostDataRules.internal_uid],
            [need_uid, PostDataRules.internal_uid],
        ])
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "input validation failed"
            return {
                RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results,
            }

        skill_uid = long(skill_uid)
        need_uid = long(need_uid)

        existings_keys = [
            ndb.Key(Datastores.caretaker_skills._get_kind(), skill_uid),
            ndb.Key(Datastores.needs._get_kind(), need_uid),
        ]

        for existing_key in existings_keys:
            call_result = DSF.kget(existing_key)
            debug_data.append(call_result)
            if call_result[RDK.success] != RC.success:
                return_msg += "Datastore access failed"
                return {
                    RDK.success: RC.datastore_failure, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
            if not call_result['get_result']:
                return_msg += "{} not found".format(existing_key.kind())
                return {
                    RDK.success: RC.input_validation_failed, RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                    'task_results': task_results,
                }
        # </end> verify input data

        joins = Datastores.skills_satisfies_needs(id=skill_uid)
        joins.need_uid = need_uid
        call_result = joins.kput()
        debug_data.append(call_result)
        if call_result[RDK.success] != RC.success:
            return_msg += "failed to write skills_satisfies_needs to datastore"
            return {
                RDK.success: call_result[RDK.success], RDK.return_msg: return_msg, RDK.debug_data: debug_data,
                'task_results': task_results
            }

        task_results['uid'] = call_result['put_result'].id()

        return {RDK.success: RC.success, RDK.return_msg: return_msg, RDK.debug_data: debug_data, 'task_results': task_results}


app = webapp2.WSGIApplication([
    (Services.modify_joins.add_modify_cluster_user.url, AddModifyClusterUser),
    (Services.modify_joins.remove_user_from_cluster.url, RemoveUserFromCluster),
    (Services.modify_joins.add_modify_user_skill.url, AddModifyUserSkill),
    (Services.modify_joins.add_modify_need_to_needer.url, AddModifyNeedToNeeder),
    (Services.modify_joins.remove_need_from_needer.url, RemoveNeedFromNeeder),
    (Services.modify_joins.remove_needer_from_user.url, RemoveNeederFromUser),
    (Services.modify_joins.assign_hashtag_to_user.url, AssignHashtagToUser),
    (Services.modify_joins.remove_hashtag_from_user.url, RemoveHashtagFromUser),
    (Services.modify_joins.remove_skill_from_user.url, RemoveSkillFromUser),
    (Services.modify_joins.modify_user_information.url, ModifyUserInformation),
    (Services.modify_joins.associate_skill_with_need.url, AssociateSkillWithNeed),
], debug=True)
