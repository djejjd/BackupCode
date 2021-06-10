<template>
  <el-row :gutter="40" class="panel-group">
    <el-col :xs="12" :sm="12" :lg="6" class="card-panel-col">
      <div class="card-panel-description">
        <div slot="header" class="header-panel-text">
          检测异常费用
        </div>
        <template>
          <el-autocomplete
            class="inline-input"
            v-model="state"
            :fetch-suggestions="panelGroupQuerySearch"
            placeholder="请输入疾病名"
            :trigger-on-focus="false"
            @select="handleSelect"
            style="width: 200px;"
          ></el-autocomplete>
          <el-button class="filter-item" type="primary" icon="el-icon-search" @click="sendDiseaseCode">
            搜索
          </el-button>
        </template>
      </div>
    </el-col>
    <el-col :xs="12" :sm="12" :lg="6" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-people">
          <svg-icon icon-class="peoples" class-name="card-panel-icon" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            疾病名
          </div>
          <template>
            <span class="card-panel-num">
              {{ this.groupChartQuery.diseaseNameKey }}
            </span>
          </template>
        </div>
      </div>
    </el-col>
    <el-col :xs="12" :sm="12" :lg="6" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-message">
          <svg-icon icon-class="message" class-name="card-panel-icon" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            疾病编码
          </div>
          <template>
            <span class="card-panel-num">
              {{this.groupChartQuery.diseaseCodeKey}}
            </span>
          </template>
        </div>
      </div>
    </el-col>
    <el-col :xs="12" :sm="12" :lg="6" class="card-panel-col">
      <div class="card-panel">
        <div class="card-panel-icon-wrapper icon-money">
          <svg-icon icon-class="money" class-name="card-panel-icon" />
        </div>
        <div class="card-panel-description">
          <div class="card-panel-text">
            平均费用
          </div>
          <template>
            <span class="card-panel-num"> {{ this.averageFee + "元"}} </span>
          </template>
        </div>
      </div>
    </el-col>
  </el-row>
</template>

<script>
import CountTo from 'vue-count-to'

import {getDataDiseaseCodeChart} from "@/api/get-chart";

export default {
  components: {
    CountTo
  },
  data() {
    return {
      products: undefined,
      list: null,
      diseaseInfo: [],
      averageFee: 0,
      groupChartQuery: {
        diseaseNameKey: "",
        diseaseCodeKey: ""
      },
      restaurants: [],
      state: '',
    }
  },
  mounted() {
    this.loadAll();
    this.bus.$on("sendDiseaseAverageFee", (data) => {
      this.averageFee = data
    })
  },
  methods: {
    // products: undefined,
    panelGroupQuerySearch(queryString, cb) {
      const restaurants = this.restaurants;
      const results = queryString ? this.createFilter(queryString) : restaurants;
      // 调用 callback 返回建议列表的数据
      cb(results);
    },
    createFilter(queryString) {
      return this.restaurants.filter(function (product) {
        return Object.keys(product).some(function (key) {
          return String(product[key]).toUpperCase().indexOf(queryString) > -1
        })
      })
      return this.products
      // return (restaurant) => {
      //   return (restaurant.value.toLowerCase().indexOf(queryString.toLowerCase()) > -1);
      // };
    },
    loadAll() {
      getDataDiseaseCodeChart(this.groupChartQuery).then(response => {
        this.restaurants = response.data
        // let data = [
        //   { "value": "慢性活动性肝炎", "address": "10001" },
        //   { "value": "非典型性分枝杆菌感染", "address": "10002" },
        //   { "value": "猩红热", "address": "10003" },
        //   { "value": "特指部位的陈旧性结核", "address": "10004" },
        // ];
        // console.log(data);
      })
    },
    handleSelect(item) {
      this.groupChartQuery.diseaseCodeKey = item['address']
      this.groupChartQuery.diseaseNameKey = item['value']
    },
    change() {
      this.$forceUpdate()
    },
    sendDiseaseCode() {
      console.log("发送")
      this.bus.$emit('sendDiseaseInfo', this.groupChartQuery)
    }
  }
}
</script>

<style lang="scss" scoped>
.panel-group {
  margin-top: 18px;

  .card-panel-col {
    margin-bottom: 32px;
  }

  .card-panel {
    height: 108px;
    cursor: pointer;
    font-size: 12px;
    position: relative;
    overflow: hidden;
    color: #666;
    background: #fff;
    box-shadow: 4px 4px 40px rgba(0, 0, 0, .05);
    border-color: rgba(0, 0, 0, .05);

    &:hover {
      .card-panel-icon-wrapper {
        color: #fff;
      }

      .icon-people {
        background: #40c9c6;
      }

      .icon-message {
        background: #36a3f7;
      }

      .icon-money {
        background: #f4516c;
      }

      .icon-shopping {
        background: #34bfa3
      }
    }

    .icon-people {
      color: #40c9c6;
    }

    .icon-message {
      color: #36a3f7;
    }

    .icon-money {
      color: #f4516c;
    }

    .icon-shopping {
      color: #34bfa3
    }

    .card-panel-icon-wrapper {
      float: left;
      margin: 14px 0 0 14px;
      padding: 16px;
      transition: all 0.38s ease-out;
      border-radius: 6px;
    }

    .card-panel-icon {
      float: left;
      font-size: 48px;
    }

    .card-panel-description {
      float: right;
      font-weight: bold;
      margin: 26px 26px 26px 0px;

      .card-panel-text {
        line-height: 18px;
        color: rgba(0, 0, 0, 0.45);
        font-size: 16px;
        margin-bottom: 12px;
      }

      .card-panel-num {
        font-size: 20px;
      }
    }
  }
}
.header-panel-text {
  line-height: 18px;
  color: rgba(0, 0, 0, 0.45);
  font-size: 16px;
  margin-bottom: 12px;
  font-weight: bold;
}

@media (max-width:550px) {
  .card-panel-description {
    display: none;
  }

  .card-panel-icon-wrapper {
    float: none !important;
    width: 100%;
    height: 100%;
    margin: 0 !important;

    .svg-icon {
      display: block;
      margin: 14px auto !important;
      float: none !important;
    }
  }
}
</style>
