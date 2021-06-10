<template>
  <div class="app-container">
    <el-button type="primary" @click="handleAddRole">增加用户</el-button>

    <el-table :data="rolesList" style="width: 100%;margin-top:30px;" border>
      <el-table-column align="center" label="身份" width="220">
        <template slot-scope="scope">
          {{ scope.row.key }}
        </template>
      </el-table-column>
      <el-table-column align="center" label="用户名" width="220">
        <template slot-scope="scope">
          {{ scope.row.name }}
        </template>
      </el-table-column>
      <el-table-column align="header-center" label="密码">
        <template slot-scope="scope">
          {{ scope.row.password }}
        </template>
      </el-table-column>
      <el-table-column align="center" label="操作">
        <template slot-scope="scope">
          <el-button type="primary" size="small" @click="handleEdit(scope)">Edit</el-button>
          <el-button type="danger" size="small" @click="handleDelete(scope)">Delete</el-button>
        </template>
      </el-table-column>
    </el-table>

    <el-dialog :visible.sync="dialogVisible" :title="dialogType==='edit'?'修改用户信息':'增加用户'">
      <el-form :model="role" label-width="80px" label-position="left">
        <el-form-item label="用户名">
          <el-input v-model="role.name" placeholder="输入用户名" :readonly="dialogType !== 'edit' ? false: 'readonly'"/>
        </el-form-item>
        <el-form-item label="密码">
          <el-input v-model="role.password" placeholder="输入密码" />
        </el-form-item>
      </el-form>
      <div style="text-align:right;">
        <el-button type="danger" @click="dialogVisible=false">取消</el-button>
        <el-button type="primary" @click="confirmRole">确定</el-button>
      </div>
    </el-dialog>
  </div>
</template>

<script>
import path from 'path'
import { deepClone } from '@/utils'
// import { getRoutes, getRoles, addRole, deleteRole, updateRole } from '@/api/role'
import {getRole, deleteRole, addRole, updateRole} from "@/api/get-chart";

const defaultRole = {
  key: '',
  name: '',
  description: '',
  routes: []
}

export default {
  data() {
    return {
      role: Object.assign({}, defaultRole),
      routes: [],
      rolesList: [],
      dialogVisible: false,
      dialogType: 'new',
      checkStrictly: false,
      defaultProps: {
        children: 'children',
        label: 'title'
      },
      userInfo: {
        "username": "",
        "password": ""
      }
    }
  },
  computed: {
    routesData() {
      return this.routes
    }
  },
  created() {
    // Mock: get all routes and roles list from server
    // this.getRoutes()
    this.getRole()
  },
  methods: {

    async getRole() {
      const res = await getRole()
      this.rolesList = res.data
    },

    // Reshape the routes structure so that it looks the same as the sidebar
    generateRoutes(routes, basePath = '/') {
      const res = []

      for (let route of routes) {
        // skip some route
        if (route.hidden) { continue }

        const onlyOneShowingChild = this.onlyOneShowingChild(route.children, route)

        if (route.children && onlyOneShowingChild && !route.alwaysShow) {
          route = onlyOneShowingChild
        }

        const data = {
          path: path.resolve(basePath, route.path),
          title: route.meta && route.meta.title

        }

        // recursive child routes
        if (route.children) {
          data.children = this.generateRoutes(route.children, data.path)
        }
        res.push(data)
      }
      return res
    },
    generateArr(routes) {
      let data = []
      routes.forEach(route => {
        data.push(route)
        if (route.children) {
          const temp = this.generateArr(route.children)
          if (temp.length > 0) {
            data = [...data, ...temp]
          }
        }
      })
      return data
    },
    handleAddRole() {
      this.role = Object.assign({}, defaultRole)
      if (this.$refs.tree) {
        this.$refs.tree.setCheckedNodes([])
      }
      this.dialogType = 'new'
      this.dialogVisible = true
    },
    handleEdit(scope) {
      this.dialogType = 'edit'
      this.dialogVisible = true
      this.checkStrictly = true
      this.role = deepClone(scope.row)
      // this.$nextTick(() => {
      //   const routes = this.generateRoutes(this.role.routes)
      //   this.$refs.tree.setCheckedNodes(this.generateArr(routes))
      //   // set checked state of a node not affects its father and child nodes
      //   this.checkStrictly = false
      // })
    },
    handleDelete({ $index, row }) {
      this.userInfo.username = row.name
      this.$confirm('确定删除这个角色?', '警告', {
        confirmButtonText: '确定',
        cancelButtonText: '取消 ',
        type: 'warning'
      })
        .then(async() => {
          let res = await deleteRole(this.userInfo)
          console.log(res)
          if (res.data === 1) {
            this.rolesList.splice($index, 1)
            this.$message({
              type: 'success',
              message: '删除成功!'
            })
          }
          else {
            this.$message({
              type: 'error',
              message: '删除失败!'
            })
          }
        })
        .catch(err => { console.error(err) })
    },
    generateTree(routes, basePath = '/', checkedKeys) {
      const res = []

      for (const route of routes) {
        const routePath = path.resolve(basePath, route.path)

        // recursive child routes
        if (route.children) {
          route.children = this.generateTree(route.children, routePath, checkedKeys)
        }

        if (checkedKeys.includes(routePath) || (route.children && route.children.length >= 1)) {
          res.push(route)
        }
      }
      return res
    },
    async confirmRole() {
      const isEdit = this.dialogType === 'edit'
      this.userInfo.username = this.role.name
      this.userInfo.password = this.role.password
      // const checkedKeys = this.$refs.tree.getCheckedKeys()
      // this.role.routes = this.generateTree(deepClone(this.serviceRoutes), '/', checkedKeys)

      if (isEdit) {
        let ret = await updateRole(this.userInfo)
        if (ret.data === 1) {
          const { key, name } = this.role
          this.dialogVisible = false
          this.$notify({
            title: 'Success',
            dangerouslyUseHTMLString: true,
            message: `
            <div>Role Key: ${key}</div>
            <div>Role Name: ${name}</div>
          `,
            type: 'success'
          })

          for (let index = 0; index < this.rolesList.length; index++) {
            if (this.rolesList[index].name === this.role.name) {
              this.rolesList.splice(index, 1, Object.assign({}, this.role))
              break
            }
          }
        } else {
          this.$message({
            type: '错误',
            message: '修改失败!'
          })
        }
      } else {
        const ret = await addRole(this.userInfo)
        console.log(this.role)
        this.role.key = "普通用户"
        this.rolesList.push(this.role)

        if (ret.data === 1) {
          const { description, key, name } = this.role
          this.dialogVisible = false
          this.$notify({
            title: 'Success',
            dangerouslyUseHTMLString: true,
            message: `
            <div>Role Key: ${key}</div>
            <div>Role Name: ${name}</div>
          `,
            type: 'success'
          })
        } else {
          this.$message({
            type: '错误',
            message: '添加失败!'
          })
        }
      }

    },
    // reference: src/view/layout/components/Sidebar/SidebarItem.vue
    onlyOneShowingChild(children = [], parent) {
      let onlyOneChild = null
      const showingChildren = children.filter(item => !item.hidden)

      // When there is only one child route, the child route is displayed by default
      if (showingChildren.length === 1) {
        onlyOneChild = showingChildren[0]
        onlyOneChild.path = path.resolve(parent.path, onlyOneChild.path)
        return onlyOneChild
      }

      // Show parent if there are no child route to display
      if (showingChildren.length === 0) {
        onlyOneChild = { ... parent, path: '', noShowingChildren: true }
        return onlyOneChild
      }

      return false
    }
  }
}
</script>

<style lang="scss" scoped>
.app-container {
  .roles-table {
    margin-top: 30px;
  }
  .permission-tree {
    margin-bottom: 30px;
  }
}
</style>
