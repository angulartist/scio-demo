const WebSocket = require('ws')
const {PubSub} = require('@google-cloud/pubsub')
const ws = new WebSocket('ws://stream.meetup.com/2/rsvps')
const pSub = new PubSub({projectId: 'XXX'})
const topicName = 'demo-scala'

const publisher = pSub
    .topic(topicName, {
        batching: {
            maxMessages: 20,
            maxMilliseconds: 5000
        }
    })

async function addToBatch(
    dataBuffer,
    customAttributes
) {
    try {
        const messageId = await publisher.publish(dataBuffer, customAttributes)
        console.log(`published ${messageId}`)
    } catch (e) {
        throw new Error('Could not publish.' + e)
    }
}

ws.on('message', incoming = async (data) => {
    const strIt = (e) => e.toString()

    const obj = JSON.parse(data)

    const item = {
        eventId: obj['rsvp_id'],
        timestamp: obj['mtime'],
        group: obj['group']
    }

    const base64str = Buffer.from(JSON.stringify(item), 'utf8')

    const customAttr = {
        event_id: strIt(item['eventId']),
        timestamp: strIt(item['timestamp'])
    }

    try {
        await addToBatch(base64str, customAttr)
    } catch (e) {
        throw new Error(e)
    }
})